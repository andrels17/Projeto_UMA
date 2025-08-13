import streamlit as st
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime, date
import os
import plotly.express as px
import hashlib

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(SCRIPT_DIR, "frotas_data.db")

ALERTAS_MANUTENCAO = {
    'HORAS': { 'default': 20 },
    'QUILÔMETROS': { 'default': 500 }
}

def formatar_brasileiro(valor: float, prefixo='') -> str:
    """Formata um número com casas decimais para o padrão brasileiro."""
    if pd.isna(valor) or not np.isfinite(valor):
        return "–"
    return f"{prefixo}{valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

@st.cache_data(ttl=300)
def para_csv(df: pd.DataFrame):
    """Converte um DataFrame para CSV para download."""
    return df.to_csv(index=False, sep=';', decimal=',').encode('utf-8-sig')

def formatar_brasileiro_int(valor: float) -> str:
    """Formata um número inteiro para o padrão brasileiro (ex: 123.456)."""
    if pd.isna(valor) or not np.isfinite(valor):
        return "–"
    return f"{int(valor):,}".replace(",", ".")

def detect_equipment_type(df_completo: pd.DataFrame) -> pd.DataFrame:
    df = df_completo.copy()
    df['Tipo_Controle'] = df.get('Unid', pd.Series(index=df.index)).map({'HORAS': 'HORAS', 'QUILÔMETROS': 'QUILÔMETROS'})
    def inferir_tipo_por_classe(row):
        if pd.notna(row['Tipo_Controle']): return row['Tipo_Controle']
        classe = str(row.get('Classe_Operacional', '')).upper()
        if any(p in classe for p in ['TRATOR', 'COLHEITADEIRA', 'PULVERIZADOR', 'PLANTADEIRA', 'PÁ CARREGADEIRA', 'RETROESCAVADEIRA']): return 'HORAS'
        if any(p in classe for p in ['CAMINHÃO', 'CAMINHAO', 'VEICULO', 'PICKUP', 'CAVALO MECANICO']): return 'QUILÔMETROS'
        return 'HORAS'
    df['Tipo_Controle'] = df.apply(inferir_tipo_por_classe, axis=1)
    return df

def hash_password(password):
    """Gera um hash seguro da palavra-passe."""
    return hashlib.sha256(password.encode()).hexdigest()

def check_login_db(username, password):
    """Verifica as credenciais contra a base de dados."""
    try:
        conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        cursor = conn.cursor()
        cursor.execute("SELECT password_hash, role FROM utilizadores WHERE username = ?", (username,))
        result = cursor.fetchone()
        conn.close()
        if result:
            password_hash_db, role = result
            if password_hash_db == hash_password(password):
                return role
        return None
    except Exception as e:
        st.error(f"Erro ao aceder à base de dados de utilizadores: {e}")
        return None

def get_all_users():
    """Busca todos os utilizadores da base de dados."""
    with sqlite3.connect(DB_PATH, check_same_thread=False) as conn:
        return pd.read_sql_query("SELECT id, username, role FROM utilizadores", conn)

def add_user(username, password, role):
    """Adiciona um novo utilizador à base de dados."""
    try:
        conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO utilizadores (username, password_hash, role) VALUES (?, ?, ?)",
            (username, hash_password(password), role)
        )
        conn.commit()
        conn.close()
        return True, "Utilizador adicionado com sucesso!"
    except sqlite3.IntegrityError:
        return False, f"Erro: O nome de utilizador '{username}' já existe."
    except Exception as e:
        return False, f"Ocorreu um erro: {e}"

def update_user(user_id, new_username, new_role):
    """Atualiza o nome e a função de um utilizador."""
    try:
        conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        cursor = conn.cursor()
        cursor.execute(
            "UPDATE utilizadores SET username = ?, role = ? WHERE id = ?",
            (new_username, new_role, user_id)
        )
        conn.commit()
        conn.close()
        return True, "Utilizador atualizado com sucesso!"
    except Exception as e:
        return False, f"Ocorreu um erro: {e}"

def delete_user(user_id):
    """Remove um utilizador da base de dados."""
    try:
        conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        cursor = conn.cursor()
        cursor.execute("DELETE FROM utilizadores WHERE id = ?", (user_id,))
        conn.commit()
        conn.close()
        return True, "Utilizador removido com sucesso!"
    except Exception as e:
        return False, f"Ocorreu um erro: {e}"

    
# APAGUE A SUA FUNÇÃO "load_data_from_db" INTEIRA E SUBSTITUA-A POR ESTE BLOCO FINAL

@st.cache_data(show_spinner="Carregando e processando dados...", ttl=300)
def load_data_from_db(db_path: str, ver_frotas: int=None, ver_abast: int=None, ver_manut: int=None, ver_comp: int=None, ver_chk: int=None):
    if not os.path.exists(db_path):
        st.error(f"Arquivo de banco de dados '{db_path}' não encontrado.")
        st.stop()

    try:
        with sqlite3.connect(db_path, check_same_thread=False) as conn:
            df_abast = pd.read_sql_query("SELECT rowid, * FROM abastecimentos", conn)
            df_frotas = pd.read_sql_query("SELECT * FROM frotas", conn)
            df_manutencoes = pd.read_sql_query("SELECT rowid, * FROM manutencoes", conn)
            df_comp_regras = pd.read_sql_query("SELECT * FROM componentes_regras", conn)
            df_comp_historico = pd.read_sql_query("SELECT * FROM componentes_historico", conn)
            df_checklist_regras = pd.read_sql_query("SELECT * FROM checklist_regras", conn)
            df_checklist_itens = pd.read_sql_query("SELECT * FROM checklist_itens", conn)
            df_checklist_historico = pd.read_sql_query("SELECT * FROM checklist_historico", conn)

        # --- Início do Processamento Integrado ---
        
        # Renomeia colunas para um padrão consistente
        df_abast = df_abast.rename(columns={"Cód. Equip.": "Cod_Equip", "Qtde Litros": "Qtde_Litros", "Mês": "Mes", "Média": "Media"}, errors='ignore')
        df_frotas = df_frotas.rename(columns={"COD_EQUIPAMENTO": "Cod_Equip", "Classe Operacional": "Classe_Operacional"}, errors='ignore')

        # Cria o dataframe principal mesclando abastecimentos e frotas
        df_merged = pd.merge(df_abast, df_frotas, on="Cod_Equip", how="left")
        
        # Trata colunas de classe operacional que podem ter vindo da mesclagem
        if 'Classe_Operacional_x' in df_merged.columns:
            df_merged['Classe_Operacional'] = np.where(df_merged['Classe_Operacional_x'].notna(), df_merged['Classe_Operacional_x'], df_merged['Classe_Operacional_y'])
            df_merged.drop(columns=['Classe_Operacional_x', 'Classe_Operacional_y'], inplace=True)
        
        # Converte a coluna de data e cria colunas de tempo
        df_merged["Data"] = pd.to_datetime(df_merged["Data"], errors='coerce')
        df_merged.dropna(subset=["Data"], inplace=True)
        df_merged["Ano"] = df_merged["Data"].dt.year
        df_merged["AnoMes"] = df_merged["Data"].dt.to_period("M").astype(str)
        
        # Limpa e converte colunas numéricas
        for col in ["Qtde_Litros", "Media", "Hod_Hor_Atual"]:
            if col in df_merged.columns:
                series = df_merged[col].astype(str)
                series = series.str.replace(',', '.', regex=False).str.replace('-', '', regex=False).str.strip()
                df_merged[col] = pd.to_numeric(series, errors='coerce')
        
        # Cria a coluna "label" no dataframe de frotas para uso em seletores
        df_frotas["label"] = df_frotas["Cod_Equip"].astype(str) + " - " + df_frotas.get("DESCRICAO_EQUIPAMENTO", "").fillna("") + " (" + df_frotas.get("PLACA", "").fillna("Sem Placa") + ")"
        
        # Garante que a classe operacional em df_frotas está atualizada
        classe_map = df_merged.dropna(subset=['Classe_Operacional']).groupby('Cod_Equip')['Classe_Operacional'].first()
        df_frotas['Classe_Operacional'] = df_frotas['Cod_Equip'].map(classe_map).fillna(df_frotas.get('Classe_Operacional'))

        # Determina o tipo de controle (Horas ou Quilômetros) para cada equipamento
        def determinar_tipo_controle(row):
            texto_para_verificar = (
                str(row.get('DESCRICAO_EQUIPAMENTO', '')) + ' ' + 
                str(row.get('Classe_Operacional', ''))
            ).upper()
            km_keywords = ['CAMINH', 'VEICULO', 'PICKUP', 'CAVALO MECANICO']
            if any(p in texto_para_verificar for p in km_keywords):
                return 'QUILÔMETROS'
            return 'HORAS'
        df_frotas['Tipo_Controle'] = df_frotas.apply(determinar_tipo_controle, axis=1)

        # Retorna todos os dataframes processados
        return (
            df_merged, df_frotas, df_manutencoes,
            df_comp_regras, df_comp_historico,
            df_checklist_regras, df_checklist_itens, df_checklist_historico
        )

    except Exception as e:
        st.error(f"Erro ao ler e processar o banco de dados: {e}")
        st.stop()
        # Retorna dataframes vazios em caso de erro
        return (pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame(),
                pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame())

                
    
def inserir_abastecimento(db_path: str, dados: dict) -> bool:
    try:
        conn = sqlite3.connect(db_path, check_same_thread=False)
        cursor = conn.cursor()
        sql = """
            INSERT INTO abastecimentos (
                "Cód. Equip.", Data, "Qtde Litros", Hod_Hor_Atual,
                Safra, "Mês", "Classe Operacional"
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """
        valores = (
            dados['cod_equip'],
            dados['data'],
            dados['qtde_litros'],
            dados['hod_hor_atual'],
            dados['safra'],
            dados['mes'],
            dados['classe_operacional']
        )
        cursor.execute(sql, valores)
        conn.commit()
        conn.close()
        return True
    except sqlite3.Error as e:
        st.error(f"Erro ao inserir dados no banco de dados: {e}")
        return False

def excluir_abastecimento(db_path: str, rowid: int) -> bool:
    """Exclui um registro de abastecimento do banco de dados usando seu rowid."""
    try:
        conn = sqlite3.connect(db_path, check_same_thread=False)
        cursor = conn.cursor()
        # Usar rowid é a forma mais segura de deletar uma linha específica
        sql = "DELETE FROM abastecimentos WHERE rowid = ?"
        cursor.execute(sql, (rowid,))
        conn.commit()
        conn.close()
        return True
    except sqlite3.Error as e:
        st.error(f"Erro ao excluir dados do banco de dados: {e}")
        return False

def inserir_manutencao(db_path: str, dados: dict) -> bool:
    try:
        conn = sqlite3.connect(db_path, check_same_thread=False)
        cursor = conn.cursor()
        sql = 'INSERT INTO manutencoes (Cod_Equip, Data, Tipo_Servico, Hod_Hor_No_Servico) VALUES (?, ?, ?, ?)'
        params = (dados['cod_equip'], dados['data'], dados['tipo_servico'], dados['hod_hor_servico'])
        cursor.execute(sql, params)
        conn.commit()
        conn.close()
        return True
    except sqlite3.Error as e:
        st.error(f"Erro no banco de dados: {e}")
        return False

def inserir_frota(db_path: str, dados: dict) -> bool:
    """Insere um novo registro de frota no banco de dados."""
    try:
        conn = sqlite3.connect(db_path, check_same_thread=False)
        cursor = conn.cursor()
        sql = """
            INSERT INTO frotas (
                COD_EQUIPAMENTO, DESCRICAO_EQUIPAMENTO, PLACA, 
                "Classe Operacional", ATIVO
            ) VALUES (?, ?, ?, ?, ?)
        """
        valores = (
            dados['cod_equip'],
            dados['descricao'],
            dados['placa'],
            dados['classe_op'],
            dados['ativo']
        )
        cursor.execute(sql, valores)
        conn.commit()
        conn.close()
        return True
    except sqlite3.Error as e:
        st.error(f"Erro no banco de dados: {e}")
        return False
    

def editar_abastecimento(db_path: str, rowid: int, dados: dict) -> bool:
    """Atualiza um registro de abastecimento existente."""
    try:
        conn = sqlite3.connect(db_path, check_same_thread=False)
        cursor = conn.cursor()
        sql = """
            UPDATE abastecimentos SET
                "Cód. Equip." = ?, Data = ?, "Qtde Litros" = ?, Hod_Hor_Atual = ?, Safra = ?
            WHERE rowid = ?
        """
        valores = (dados['cod_equip'], dados['data'], dados['qtde_litros'], dados['hod_hor_atual'], dados['safra'], rowid)
        cursor.execute(sql, valores)
        conn.commit()
        conn.close()
        return True
    except sqlite3.Error as e:
        st.error(f"Erro ao atualizar abastecimento: {e}")
        return False

def editar_manutencao(db_path: str, rowid: int, dados: dict) -> bool:
    """Atualiza um registro de manutenção existente."""
    try:
        conn = sqlite3.connect(db_path, check_same_thread=False)
        cursor = conn.cursor()
        sql = """
            UPDATE manutencoes SET
                Cod_Equip = ?, Data = ?, Tipo_Servico = ?, Hod_Hor_No_Servico = ?
            WHERE rowid = ?
        """
        valores = (dados['cod_equip'], dados['data'], dados['tipo_servico'], dados['hod_hor_servico'], rowid)
        cursor.execute(sql, valores)
        conn.commit()
        conn.close()
        return True
    except sqlite3.Error as e:
        st.error(f"Erro ao atualizar manutenção: {e}")
        return False

def importar_abastecimentos_de_planilha(db_path: str, arquivo_carregado) -> tuple[int, int, str]:
    """Lê uma planilha, verifica por duplicados, e insere os novos dados."""
    try:
        df_novo = pd.read_excel(arquivo_carregado)
        
        # Mapeamento das colunas (ajuste se necessário)
        mapa_colunas = {
            "Cód. Equip.": "Cód. Equip.",
            "Data": "Data",
            "Qtde Litros": "Qtde Litros",
            "Hod. Hor. Atual": "Hod_Hor_Atual",
            "Safra": "Safra",
            "Mês": "Mês",
            "Classe Operacional": "Classe Operacional"
        }
        df_novo = df_novo.rename(columns=mapa_colunas)

        colunas_necessarias = list(mapa_colunas.values())
        colunas_faltando = [col for col in colunas_necessarias if col not in df_novo.columns]
        if colunas_faltando:
            return 0, 0, f"Erro: Colunas não encontradas: {', '.join(colunas_faltando)}"
        conn = sqlite3.connect(db_path)
        df_existente = pd.read_sql_query("SELECT * FROM abastecimentos", conn)
        
        df_novo['Data'] = pd.to_datetime(df_novo['Data']).dt.strftime('%Y-%m-%d %H:%M:%S')
        df_existente['Data'] = pd.to_datetime(df_existente['Data']).dt.strftime('%Y-%m-%d %H:%M:%S')

        df_novo['chave_unica'] = df_novo['Cód. Equip.'].astype(str) + '_' + df_novo['Data'] + '_' + df_novo['Qtde Litros'].astype(str)
        df_existente['chave_unica'] = df_existente['Cód. Equip.'].astype(str) + '_' + df_existente['Data'] + '_' + df_existente['Qtde Litros'].astype(str)

        df_para_inserir = df_novo[~df_novo['chave_unica'].isin(df_existente['chave_unica'])]
        
        num_duplicados = len(df_novo) - len(df_para_inserir)

        if df_para_inserir.empty:
            return 0, num_duplicados, "Nenhum registo novo para importar. Todos os registos da planilha já existem na base de dados."

        df_para_inserir_final = df_para_inserir[colunas_necessarias]
        registros = [tuple(x) for x in df_para_inserir_final.to_numpy()]
        
        cursor = conn.cursor()
        sql = f"INSERT INTO abastecimentos ({', '.join(f'\"{col}\"' for col in colunas_necessarias)}) VALUES (?, ?, ?, ?, ?, ?, ?)"
        cursor.executemany(sql, registros)
        
        conn.commit()
        num_inseridos = cursor.rowcount
        conn.close()
        
        mensagem_sucesso = f"{num_inseridos} registos novos foram importados com sucesso."
        if num_duplicados > 0:
            mensagem_sucesso += f" {num_duplicados} registos duplicados foram ignorados."
            
        return num_inseridos, num_duplicados, mensagem_sucesso

    except Exception as e:
        return 0, 0, f"Ocorreu um erro inesperado durante a importação: {e}"

def editar_frota(db_path: str, cod_equip: int, dados: dict) -> bool:
    """Atualiza um registro de frota existente."""
    try:
        conn = sqlite3.connect(db_path, check_same_thread=False)
        cursor = conn.cursor()
        sql = """
            UPDATE frotas SET
                DESCRICAO_EQUIPAMENTO = ?, PLACA = ?, "Classe Operacional" = ?, ATIVO = ?
            WHERE COD_EQUIPAMENTO = ?
        """
        valores = (dados['descricao'], dados['placa'], dados['classe_op'], dados['ativo'], cod_equip)
        cursor.execute(sql, valores)
        conn.commit()
        conn.close()
        return True
    except sqlite3.Error as e:
        st.error(f"Erro ao atualizar frota: {e}")
        return False

# COLE ESTE BLOCO DE CÓDIGO NO LOCAL INDICADO

def get_component_rules():
    """Busca todas as regras de componentes da base de dados."""
    with sqlite3.connect(DB_PATH) as conn:
        return pd.read_sql_query("SELECT * FROM componentes_regras", conn)

def add_component_rule(classe, componente, intervalo):
    """Adiciona uma nova regra de componente à base de dados."""
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO componentes_regras (classe_operacional, nome_componente, intervalo_padrao) VALUES (?, ?, ?)",
                (classe, componente, intervalo)
            )
            conn.commit()
        return True, f"Componente '{componente}' adicionado com sucesso à classe '{classe}'."
    except Exception as e:
        return False, f"Erro ao adicionar componente: {e}"

def delete_component_rule(rule_id):
    """Remove uma regra de componente da base de dados."""
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM componentes_regras WHERE id_regra = ?", (rule_id,))
            conn.commit()
        return True, "Componente removido com sucesso."
    except Exception as e:
        return False, f"Erro ao remover componente: {e}"

def add_component_service(cod_equip, componente, data, hod_hor, obs):
    """Adiciona um novo registo de serviço de componente ao histórico."""
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO componentes_historico (Cod_Equip, nome_componente, Data, Hod_Hor_No_Servico, Observacoes) VALUES (?, ?, ?, ?, ?)",
                (cod_equip, componente, data, hod_hor, obs)
            )
            conn.commit()
        return True, "Serviço de componente registado com sucesso."
    except Exception as e:
        return False, f"Erro ao registar serviço: {e}"

@st.cache_data(ttl=120)
def filtrar_dados(df: pd.DataFrame, opts: dict) -> pd.DataFrame:
    # Garante que a coluna de data é do tipo datetime
    df['Data'] = pd.to_datetime(df['Data'])
    
    # Filtra por período de datas
    df_filtrado = df[
        (df['Data'].dt.date >= opts['data_inicio']) & 
        (df['Data'].dt.date <= opts['data_fim'])
    ]
    
    # Filtra pelas outras seleções, se existirem
    if opts.get("classes_op"):
        df_filtrado = df_filtrado[df_filtrado["Classe_Operacional"].isin(opts["classes_op"])]
    
    if opts.get("safras"):
        df_filtrado = df_filtrado[df_filtrado["Safra"].isin(opts["safras"])]
        
    return df_filtrado.copy()

@st.cache_data(show_spinner="Calculando plano de manutenção...", ttl=300)
def build_component_maintenance_plan(_df_frotas: pd.DataFrame, _df_abastecimentos: pd.DataFrame, _df_componentes_regras: pd.DataFrame, _df_componentes_historico: pd.DataFrame) -> pd.DataFrame:
    latest_readings = _df_abastecimentos.sort_values('Data').groupby('Cod_Equip')['Hod_Hor_Atual'].last()
    plan_data = []

    for _, frota_row in _df_frotas.iterrows():
        cod_equip = frota_row['Cod_Equip']
        classe_op = frota_row.get('Classe_Operacional')
        hod_hor_atual = latest_readings.get(cod_equip)

        if pd.isna(hod_hor_atual) or not classe_op:
            continue
        
        regras_da_classe = _df_componentes_regras[_df_componentes_regras['classe_operacional'] == classe_op]
        if regras_da_classe.empty:
            continue

        unidade = 'km' if frota_row['Tipo_Controle'] == 'QUILÔMETROS' else 'h'
        alerta_default = ALERTAS_MANUTENCAO.get(frota_row['Tipo_Controle'], {}).get('default', 500)
        
        record = {
            'Cod_Equip': cod_equip, 
            'Equipamento': frota_row.get('DESCRICAO_EQUIPAMENTO'), 
            'Leitura_Atual': hod_hor_atual, 
            'Unidade': unidade, 
            'Qualquer_Alerta': False, 
            'Alertas': []
        }

        for _, regra in regras_da_classe.iterrows():
            componente = regra['nome_componente']
            intervalo = regra['intervalo_padrao']
            
            historico_componente = _df_componentes_historico[
                (_df_componentes_historico['Cod_Equip'] == cod_equip) &
                (_df_componentes_historico['nome_componente'] == componente)
            ]
            
            ultimo_servico_hod_hor = 0
            if not historico_componente.empty:
                ultimo_servico_hod_hor = historico_componente['Hod_Hor_No_Servico'].max()

            prox_servico = ((ultimo_servico_hod_hor // intervalo) * intervalo) + intervalo
            while prox_servico < hod_hor_atual:
                prox_servico += intervalo

            restante = prox_servico - hod_hor_atual
            
            record[f'Restante_{componente}'] = restante
            
            if restante <= alerta_default:
                record['Qualquer_Alerta'] = True
                record['Alertas'].append(componente)

        plan_data.append(record)

    # 🔹 Garante que sempre retorna um DataFrame com as colunas básicas
    if not plan_data:
        return pd.DataFrame(columns=['Cod_Equip', 'Equipamento', 'Leitura_Atual', 'Unidade', 'Qualquer_Alerta', 'Alertas'])

    return pd.DataFrame(plan_data)


def prever_manutencoes(df_veiculos: pd.DataFrame, df_abastecimentos: pd.DataFrame, plan_df: pd.DataFrame) -> pd.DataFrame:
    """Estima as datas das próximas manutenções com base no uso médio."""
    if plan_df.empty or 'Leitura_Atual' not in plan_df.columns:
        return pd.DataFrame()

    # Calcula o uso diário médio de cada veículo
    uso_diario = {}
    for cod_equip in df_abastecimentos['Cod_Equip'].unique():
        dados_equip = df_abastecimentos[df_abastecimentos['Cod_Equip'] == cod_equip].sort_values('Data')
        if len(dados_equip) > 1:
            total_dias = (dados_equip['Data'].max() - dados_equip['Data'].min()).days
            total_uso = dados_equip['Hod_Hor_Atual'].max() - dados_equip['Hod_Hor_Atual'].min()
            if total_dias > 0 and total_uso > 0: # Garante que houve uso e passagem de tempo
                uso_diario[cod_equip] = total_uso / total_dias

    previsoes = []
    servicos_nomes = [col.replace('Restante_', '') for col in plan_df.columns if 'Restante_' in col]

    for _, row in plan_df.iterrows():
        cod_equip = row['Cod_Equip']
        uso = uso_diario.get(cod_equip)
        if uso:
            for nome_servico in servicos_nomes:
                col_restante = f'Restante_{nome_servico}'
                if col_restante in row and pd.notna(row[col_restante]):
                    dias_para_manut = row[col_restante] / uso
                    data_prevista = datetime.now() + pd.Timedelta(days=dias_para_manut)
                    previsoes.append({
                        'Equipamento': row['Equipamento'],
                        'Manutenção': nome_servico,
                        'Data Prevista': data_prevista.strftime('%d/%m/%Y'),
                        'Dias Restantes': int(dias_para_manut)
                    })

    if not previsoes:
        return pd.DataFrame()

    df_previsoes = pd.DataFrame(previsoes)
    return df_previsoes.sort_values('Dias Restantes')


# ---------------------------
# Funções para Checklists
# ---------------------------

@st.cache_data(ttl=120)
def get_checklist_rules():
    """Busca todas as regras de checklist do banco de dados."""
    try:
        with sqlite3.connect(DB_PATH, check_same_thread=False) as conn:
            return pd.read_sql_query("SELECT * FROM checklist_regras", conn)
    except Exception as e:
        st.error(f"Erro ao buscar regras de checklist: {e}")
        return pd.DataFrame()

@st.cache_data(ttl=120)
def get_checklist_items(id_regra):
    """Busca os itens de checklist para uma determinada regra."""
    try:
        with sqlite3.connect(DB_PATH, check_same_thread=False) as conn:
            return pd.read_sql_query(
                "SELECT * FROM checklist_itens WHERE id_regra = ?",
                conn,
                params=(id_regra,)
            )
    except Exception as e:
        st.error(f"Erro ao buscar itens de checklist: {e}")
        return pd.DataFrame()


# ---------------------------
# CRUD para Checklists
# ---------------------------

def add_checklist_rule(classe_operacional, titulo_checklist, turno, frequencia):
    """Adiciona uma nova regra de checklist ao banco de dados."""
    try:
        with sqlite3.connect(DB_PATH, check_same_thread=False) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO checklist_regras (classe_operacional, titulo_checklist, frequencia, turno)
                VALUES (?, ?, ?, ?)
                """ ,
                (classe_operacional, titulo_checklist, frequencia, turno)
            )
            conn.commit()
        return True, "Regra de checklist adicionada com sucesso!"
    except Exception as e:
        return False, f"Erro ao adicionar regra de checklist: {e}"


def add_checklist_rule_and_get_id(classe_operacional, titulo_checklist, turno, frequencia):
    """Adiciona uma nova regra e devolve o ID criado (ou None em erro).

    Mantém a função "add_checklist_rule" para compatibilidade, mas quando for
    necessário o ID imediatamente após a criação, utilize esta função.
    """
    try:
        with sqlite3.connect(DB_PATH, check_same_thread=False) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO checklist_regras (classe_operacional, titulo_checklist, frequencia, turno)
                VALUES (?, ?, ?, ?)
                """,
                (classe_operacional, titulo_checklist, frequencia, turno)
            )
            conn.commit()
            return cursor.lastrowid
    except Exception as e:
        st.error(f"Erro ao adicionar regra de checklist: {e}")
        return None


def edit_checklist_rule(id_regra, classe_operacional, titulo_checklist, turno, frequencia):
    """Edita uma regra de checklist existente."""
    try:
        with sqlite3.connect(DB_PATH, check_same_thread=False) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                UPDATE checklist_regras
                SET classe_operacional = ?, titulo_checklist = ?, frequencia = ?, turno = ?
                WHERE id_regra = ?
                """ ,
                (classe_operacional, titulo_checklist, frequencia, turno, id_regra)
            )
            conn.commit()
        return True, "Regra de checklist atualizada com sucesso!"
    except Exception as e:
        return False, f"Erro ao editar regra de checklist: {e}"


def delete_checklist_rule(id_regra):
    """Remove uma regra de checklist e seus itens associados."""
    try:
        with sqlite3.connect(DB_PATH, check_same_thread=False) as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM checklist_itens WHERE id_regra = ?", (id_regra,))
            cursor.execute("DELETE FROM checklist_regras WHERE id_regra = ?", (id_regra,))
            conn.commit()
        return True, "Regra de checklist removida com sucesso!"
    except Exception as e:
        return False, f"Erro ao remover regra de checklist: {e}"


def add_checklist_item(id_regra, nome_item):
    """Adiciona um novo item de checklist a uma regra existente."""
    try:
        with sqlite3.connect(DB_PATH, check_same_thread=False) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO checklist_itens (id_regra, nome_item)
                VALUES (?, ?)
                """ ,
                (id_regra, nome_item)
            )
            conn.commit()
        return True, "Item de checklist adicionado com sucesso!"
    except Exception as e:
        return False, f"Erro ao adicionar item de checklist: {e}"


def edit_checklist_item(id_item, nome_item):
    """Edita um item de checklist existente."""
    try:
        with sqlite3.connect(DB_PATH, check_same_thread=False) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                UPDATE checklist_itens
                SET nome_item = ?
                WHERE id_item = ?
                """ ,
                (nome_item, id_item)
            )
            conn.commit()
        return True, "Item de checklist atualizado com sucesso!"
    except Exception as e:
        return False, f"Erro ao editar item de checklist: {e}"


def delete_checklist_item(id_item):
    """Remove um item de checklist."""
    try:
        with sqlite3.connect(DB_PATH, check_same_thread=False) as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM checklist_itens WHERE id_item = ?", (id_item,))
            conn.commit()
        return True, "Item de checklist removido com sucesso!"
    except Exception as e:
        return False, f"Erro ao remover item de checklist: {e}"


def save_checklist_history(cod_equip, titulo_checklist, data_preenchimento, turno, status_geral):
    """Salva um checklist preenchido no histórico."""
    try:
        with sqlite3.connect(DB_PATH, check_same_thread=False) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO checklist_historico 
                (Cod_Equip, titulo_checklist, data_preenchimento, turno, status_geral) 
                VALUES (?, ?, ?, ?, ?)
                """ ,
                (cod_equip, titulo_checklist, data_preenchimento, turno, status_geral)
            )
            conn.commit()
    except Exception as e:
        st.error(f"Erro ao salvar histórico de checklist: {e}")


def delete_checklist_history(cod_equip, titulo_checklist, data_preenchimento, turno):
    """Remove um registro do histórico de checklists usando uma combinação única de campos."""
    try:
        with sqlite3.connect(DB_PATH, check_same_thread=False) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "DELETE FROM checklist_historico WHERE Cod_Equip = ? AND titulo_checklist = ? AND data_preenchimento = ? AND turno = ?", 
                (cod_equip, titulo_checklist, data_preenchimento, turno)
            )
            conn.commit()
            return True, "Checklist excluído com sucesso!"
    except Exception as e:
        return False, f"Erro ao excluir checklist: {e}"


def main():
    st.set_page_config(page_title="Dashboard de Frotas", layout="wide")
    # Garante tema dark coerente mesmo sem config.toml
    st.markdown(
        """
        <style>
        :root {
            --primary: #10b981;
            --bg: #0f172a;
            --bg2: #111827;
            --text: #e5e7eb;
        }
        body { background: var(--bg); color: var(--text); }
        section.main > div { background: var(--bg); }
        .stApp { background: var(--bg); }
        .st-emotion-cache-1r4qj8v, .st-emotion-cache-13ln4jf { background: var(--bg2) !important; }
        .stButton>button { background: var(--primary); color: #062e24; }
        </style>
        """,
        unsafe_allow_html=True,
    )
    # CSS fino para polir a UI
    st.markdown(
        """
        <style>
        /* Cartões/containers */
        .stExpander, .stDataFrame, .stTable { border-radius: 10px !important; }
        .stButton>button { border-radius: 8px; padding: 0.5rem 1rem; }
        .stSelectbox, .stTextInput, .stNumberInput, .stDateInput, .stTextArea { border-radius: 8px !important; }
        /* Métricas com mais destaque */
        div[data-testid="stMetric"] { background: rgba(255,255,255,0.04); padding: 10px 14px; border-radius: 12px; }
        /* Títulos com leve gradiente */
        h1, h2, h3 { background: linear-gradient(90deg, #10b981 0%, #06b6d4 100%); -webkit-background-clip: text; -webkit-text-fill-color: transparent; }
        /* Linhas divisórias mais suaves */
        hr { border: none; height: 1px; background: rgba(255,255,255,0.08); }
        /* Subtítulo de marca (opcional) */
        .brand-subtitle { display: none; }
        /* Centralizar e limitar logo na sidebar */
        section[data-testid="stSidebar"] img { display: block; margin: 0.5rem auto 0.75rem; max-width: 140px; }
        </style>
        """,
        unsafe_allow_html=True,
    )
    
    if 'authenticated' not in st.session_state:
        st.session_state.authenticated = False
        st.session_state.role = None
        st.session_state.username = ""

    if not st.session_state.authenticated:
        _ , col_central, _ = st.columns([1, 1.5, 1])
    
        with col_central:
            
            if os.path.exists("logo.png"):
                # Cria 3 sub-colunas dentro da coluna central
                _, logo_col, _ = st.columns([1, 2, 1])
                with logo_col:
                    st.image("logo.png", width=140)
            
            st.title("Bem vindo ao Aplicativo de Controle do PCMA")

            username = st.text_input("Usuário", key="login_user")
            password = st.text_input("Senha", type="password", key="login_pass")

            if st.button("Entrar", use_container_width=True):
                role = check_login_db(username, password)
                if role:
                    st.session_state.authenticated = True
                    st.session_state.role = role
                    st.session_state.username = username
                    st.rerun()
                else:
                    st.error("Usuário ou Senha incorretos.")
    else:

        # Cabeçalho com logo + título
        if os.path.exists("logo.png"):
            col_logo, col_title = st.columns([1, 6])
            with col_logo:
                st.image("logo.png", width=72)
            with col_title:
                st.title("📊 Dashboard de Frotas e Abastecimentos")
        else:
            st.title("📊 Dashboard de Frotas e Abastecimentos")

        # Passo um fingerprint simples das tabelas para invalidar cache quando necessário
        ver_frotas = int(os.path.getmtime(DB_PATH)) if os.path.exists(DB_PATH) else 0
        df, df_frotas, df_manutencoes, df_comp_regras, df_comp_historico, df_checklist_regras, df_checklist_itens, df_checklist_historico = load_data_from_db(DB_PATH, ver_frotas, ver_frotas, ver_frotas, ver_frotas, ver_frotas)
        


        if 'intervalos_por_classe' not in st.session_state:
            st.session_state.intervalos_por_classe = {}
        classes_operacionais = [c for c in df_frotas['Classe_Operacional'].unique() if pd.notna(c) and str(c).strip()]
        for classe in classes_operacionais:
            if classe not in st.session_state.intervalos_por_classe:
                tipo_controle = df_frotas[df_frotas['Classe_Operacional'] == classe]['Tipo_Controle'].iloc[0]
                if tipo_controle == 'HORAS':
                    st.session_state.intervalos_por_classe[classe] = {
                        'meta_consumo': 5.0,
                        'servicos': {
                            'servico_1': {'nome': 'Lubrificacao', 'intervalo': 250},
                            'servico_2': {'nome': 'Revisao A', 'intervalo': 100},
                            'servico_3': {'nome': 'Revisao B', 'intervalo': 300},
                            'servico_4': {'nome': 'Revisao C', 'intervalo': 500}
                        }
                    }
                else: # QUILÔMETROS
                    st.session_state.intervalos_por_classe[classe] = {
                        'meta_consumo': 2.5,
                        'servicos': {
                            'servico_1': {'nome': 'Lubrificacao', 'intervalo': 5000},
                            'servico_2': {'nome': 'Revisao 5k', 'intervalo': 5000},
                            'servico_3': {'nome': 'Revisao 10k', 'intervalo': 10000},
                            'servico_4': {'nome': 'Revisao 20k', 'intervalo': 20000}
                        }
                    }
                    
        with st.sidebar:
            if os.path.exists("logo.png"):
                st.image("logo.png", use_container_width=True)
            st.write(f"Bem-vindo, **{st.session_state.username}**!")
            if st.button("Sair"):
                st.session_state.authenticated = False
                st.session_state.username = "" # Limpa o username ao sair
                st.session_state.role = None
                st.rerun()
            st.markdown("---")

        with st.sidebar:
            st.header("📅 Filtros")

            # --- Filtro de Período (Sempre Visível) ---
            st.subheader("Período de Análise")
            data_inicio = st.date_input(
                "Data de Início", 
                df['Data'].min().date(),
                key='data_inicio'
            )
            data_fim = st.date_input(
                "Data de Fim", 
                df['Data'].max().date(),
                key='data_fim'
            )

            st.markdown("---")
            st.caption("Desenvolvido por André Luis")

            # --- NOVO: Filtro de Classe em um Menu Expansível ---
            with st.expander("Filtrar por Classe Operacional"):
                classe_opts = sorted(list(df["Classe_Operacional"].dropna().unique()))
                sel_classes = st.multiselect(
                    "Selecione as Classes", 
                    classe_opts, 
                    default=classe_opts,
                    key="sel_classes"
                )

            # --- NOVO: Filtro de Safra em um Menu Expansível ---
            with st.expander("Filtrar por Safra"):
                safra_opts = sorted(list(df["Safra"].dropna().unique()))
                sel_safras = st.multiselect(
                    "Selecione as Safras", 
                    safra_opts, 
                    default=safra_opts,
                    key="sel_safras"
                )

            # Reúne todas as opções para a função de filtro
            opts = {
                "data_inicio": data_inicio,
                "data_fim": data_fim,
                "classes_op": sel_classes, 
                "safras": sel_safras
            }
    #----------------------------------------------------- aba principal --------------------------------------
        df_f = filtrar_dados(df, opts)
        plan_df = build_component_maintenance_plan(df_frotas, df, df_comp_regras, df_comp_historico)


        abas_visualizacao = ["📊 Painel de Controle", "📈 Análise Geral", "🛠️ Controle de Manutenção", "🔎 Consulta Individual", "✅ Checklists Diários"]
        abas_admin = ["⚙️ Gerir Lançamentos", "⚙️ Gerir Frotas", "📤 Importar Dados", "⚙️ Configurações", "⚕️ Saúde dos Dados", "👤 Gerir Utilizadores", "✅ Gerir Checklists"]

        if st.session_state.role == 'admin':
            tabs_para_mostrar = abas_visualizacao + abas_admin
            active_idx = st.session_state.get('active_tab_index', 0)
            active_idx = max(0, min(active_idx, len(tabs_para_mostrar) - 1))
            try:
                abas = st.tabs(tabs_para_mostrar, default_index=active_idx)
            except TypeError:
                abas = st.tabs(tabs_para_mostrar)
            (tab_painel, tab_analise, tab_manut, tab_consulta, tab_checklists, 
            tab_gerir_lanc, tab_gerir_frotas, tab_importar, tab_config, tab_saude, 
            tab_gerir_users, tab_gerir_checklists) = abas
        else:
            tabs_para_mostrar = abas_visualizacao
            active_idx = st.session_state.get('active_tab_index', 0)
            active_idx = max(0, min(active_idx, len(tabs_para_mostrar) - 1))
            try:
                tab_painel, tab_analise, tab_manut, tab_consulta, tab_checklists = st.tabs(tabs_para_mostrar, default_index=active_idx)
            except TypeError:
                tab_painel, tab_analise, tab_manut, tab_consulta, tab_checklists = st.tabs(tabs_para_mostrar)

        def rerun_keep_tab(tab_title: str, clear_cache: bool = True):
            if clear_cache:
                st.cache_data.clear()
            try:
                st.session_state['active_tab_index'] = tabs_para_mostrar.index(tab_title)
            except Exception:
                pass
            st.rerun()
                
        with tab_painel:
            st.header("Visão Geral da Frota")
            
            kpi1, kpi2, kpi3, kpi4 = st.columns(4)
            
            # KPI 1: Frotas Ativas
            total_frotas_ativas = df_frotas[df_frotas['ATIVO'] == 'ATIVO']['Cod_Equip'].nunique()
            kpi1.metric("Frotas Ativas", total_frotas_ativas)
            
            # KPI 2: Frotas com Alerta
            frotas_com_alerta = plan_df[plan_df['Qualquer_Alerta'] == True]['Cod_Equip'].nunique() if not plan_df.empty else 0
            kpi2.metric("Frotas com Alerta", frotas_com_alerta)
            
            # KPIs 3 e 4: Frotas Mais e Menos Eficientes
            df_media_geral = df_f[(df_f['Media'].notna()) & (df_f['Media'] > 0)]
            if not df_media_geral.empty:
                # Agrupa por Código e Descrição para ter acesso a ambos
                media_por_equip = df_media_geral.groupby(['Cod_Equip', 'DESCRICAO_EQUIPAMENTO'])['Media'].mean().sort_values()
                
                if not media_por_equip.empty:
                    # Pega o CÓDIGO do mais eficiente (primeiro da lista ordenada)
                    cod_mais_eficiente = media_por_equip.index[0][0]
                    media_mais_eficiente = media_por_equip.iloc[0]
                    # Exibe o CÓDIGO no KPI
                    kpi3.metric("Frota Mais Eficiente", f"{cod_mais_eficiente}", f"{formatar_brasileiro(media_mais_eficiente)}")
            
                    # Pega o CÓDIGO do menos eficiente (último da lista ordenada)
                    cod_menos_eficiente = media_por_equip.index[-1][0]
                    media_menos_eficiente = media_por_equip.iloc[-1]
                    # Exibe o CÓDIGO no KPI
                    kpi4.metric("Frota Menos Eficiente", f"{cod_menos_eficiente}", f"{formatar_brasileiro(media_menos_eficiente)}")

            st.subheader("🏆 Ranking de Eficiência (vs. Média da Classe)")
            if 'Media' in df_f.columns and not df_f['Media'].dropna().empty:
                media_por_classe = df_f.groupby('Classe_Operacional')['Media'].mean().to_dict()
                ranking_df = df_f.copy()
                ranking_df['Media_Classe'] = ranking_df['Classe_Operacional'].map(media_por_classe)
                ranking_df['Eficiencia_%'] = ((ranking_df['Media_Classe'] / ranking_df['Media']) - 1) * 100
                
                ranking = ranking_df.groupby(['Cod_Equip', 'DESCRICAO_EQUIPAMENTO'])['Eficiencia_%'].mean().sort_values(ascending=False).reset_index()
                ranking.rename(columns={'DESCRICAO_EQUIPAMENTO': 'Equipamento', 'Eficiencia_%': 'Eficiência (%)'}, inplace=True)
                
                # --- INÍCIO DA CORREÇÃO ---
                # Cria uma nova coluna "Equipamento" que combina o Código com a Descrição
                ranking['Equipamento'] = ranking['Cod_Equip'].astype(str) + " - " + ranking['Equipamento']
                # --- FIM DA CORREÇÃO ---
            
                def formatar_eficiencia(val):
                    if pd.isna(val): return "N/A"
                    if val > 5: return f"🟢 {val:+.2f}%".replace('.',',')
                    if val < -5: return f"🔴 {val:+.2f}%".replace('.',',')
                    return f"⚪ {val:+.2f}%".replace('.',',')
                
                ranking['Eficiência (%)'] = ranking['Eficiência (%)'].apply(formatar_eficiencia)
                
                # Exibe a nova coluna "Equipamento" formatada
                st.dataframe(ranking[['Equipamento', 'Eficiência (%)']])                    
                            # NOVO: Botão de Exportação para o Ranking
                csv_ranking = para_csv(ranking)
                st.download_button("📥 Exportar Ranking para CSV", csv_ranking, "ranking_eficiencia.csv", "text/csv")
            else:
                    st.info("Não há dados de consumo médio para gerar o ranking.")
                    
            st.markdown("---")
            st.subheader("📈 Tendência de Consumo Mensal")

            if not df_f.empty and 'Qtde_Litros' in df_f.columns:
                # Agrupa os dados por Ano/Mês e soma o consumo
                consumo_mensal = df_f.groupby('AnoMes')['Qtde_Litros'].sum().reset_index().sort_values('AnoMes')
                
                if not consumo_mensal.empty:
                    fig_tendencia = px.line(
                        consumo_mensal,
                        x='AnoMes',
                        y='Qtde_Litros',
                        title="Evolução do Consumo de Combustível (Litros)",
                        labels={"AnoMes": "Mês", "Qtde_Litros": "Litros Consumidos"},
                        markers=True # Adiciona marcadores para cada mês
                    )
                    fig_tendencia.update_layout(xaxis_title="Mês/Ano", yaxis_title="Litros Consumidos")
                    st.plotly_chart(fig_tendencia, use_container_width=True)
                else:
                    st.info("Não há dados suficientes para gerar o gráfico de tendência com os filtros selecionados.")
                
        with tab_analise:
            st.header("📈 Análise Gráfica de Consumo")

            if not df_f.empty:
                if 'Media' in df_f.columns:
                    k1, k2 = st.columns(2)
                    k1.metric("Litros Consumidos (período)", formatar_brasileiro_int(df_f["Qtde_Litros"].sum()))
                    k2.metric("Média Consumo (período)", f"{formatar_brasileiro(df_f['Media'].mean())}")
                else:
                    k1.metric("Litros Consumidos (período)", formatar_brasileiro_int(df_f["Qtde_Litros"].sum()))
                st.markdown("---")
                c1, c2 = st.columns(2)

                with c1:
                    st.subheader("Consumo por Classe Operacional")
                    classes_a_excluir = ['VEICULOS LEVES', 'MOTOCICLETA', 'MINI CARREGADEIRA', 'USINA']
                    df_consumo_classe = df_f[~df_f['Classe_Operacional'].str.upper().isin(classes_a_excluir)]
                    consumo_por_classe = df_consumo_classe.groupby("Classe_Operacional")["Qtde_Litros"].sum().sort_values(ascending=False).reset_index()

                    if not consumo_por_classe.empty:
                        consumo_por_classe['texto_formatado'] = consumo_por_classe['Qtde_Litros'].apply(formatar_brasileiro_int)
                        fig_classe = px.bar(consumo_por_classe, x='Qtde_Litros', y='Classe_Operacional', orientation='h', text='texto_formatado', labels={"x": "Litros Consumidos", "y": "Classe Operacional"})
                        fig_classe.update_traces(texttemplate='%{text} L', textposition='outside')
                        fig_classe.update_layout(yaxis={'categoryorder':'total ascending'}, xaxis_title="Total Consumido (Litros)", yaxis_title="Classe Operacional")
                        st.plotly_chart(fig_classe, use_container_width=True)

                with c2:
                    st.subheader("Top 10 Equipamentos com Maior Consumo")
                    consumo_por_equip = df_f.groupby("Cod_Equip").agg({'Qtde_Litros': 'sum', 'DESCRICAO_EQUIPAMENTO': 'first'}).dropna()
                    consumo_por_equip = consumo_por_equip[consumo_por_equip.index != 550]
                    consumo_por_equip = consumo_por_equip.sort_values(by="Qtde_Litros", ascending=False).head(10)

                    if not consumo_por_equip.empty:
                        consumo_por_equip['label_grafico'] = consumo_por_equip['DESCRICAO_EQUIPAMENTO'].str.strip() + " (" + consumo_por_equip.index.astype(str) + ")"
                        consumo_por_equip['texto_formatado'] = consumo_por_equip['Qtde_Litros'].apply(formatar_brasileiro_int)
                        fig_top10 = px.bar(consumo_por_equip, x='Qtde_Litros', y='label_grafico', orientation='h', text='texto_formatado', labels={"Qtde_Litros": "Total Consumido (Litros)", "label_grafico": "Equipamento"})
                        fig_top10.update_traces(texttemplate='%{text} L', textposition='outside')
                        fig_top10.update_layout(yaxis={'categoryorder':'total ascending'}, xaxis_title="Total Consumido (Litros)", yaxis_title="Equipamento")
                        st.plotly_chart(fig_top10, use_container_width=True)
                
                st.markdown("---")
                st.subheader("Média de Consumo por Classe Operacional")

                df_media = df_f[(df_f['Media'].notna()) & (df_f['Media'] > 0)].copy()

                classes_para_excluir = ['MOTOCICLETA', 'VEICULOS LEVES', 'USINA', 'MINI CARREGADEIRA']

                df_media_filtrado = df_media[~df_media['Classe_Operacional'].str.upper().isin(classes_para_excluir)]

                if not df_media_filtrado.empty: # Usa o novo DataFrame filtrado
                    media_por_classe = df_media_filtrado.groupby('Classe_Operacional')['Media'].mean().sort_values(ascending=True)
                    
                    df_media_grafico = media_por_classe.reset_index()
                    df_media_grafico['texto_formatado'] = df_media_grafico['Media'].apply(
                        lambda x: formatar_brasileiro(x)
                    )
                    
                    # Cria o gráfico de barras
                    fig_media_classe = px.bar(
                        df_media_grafico,
                        x='Media',
                        y='Classe_Operacional',
                        orientation='h',
                        title="Média de Consumo (L/h ou Km/L) por Classe",
                        text='texto_formatado'
                    )
                    fig_media_classe.update_traces(
                        textposition='outside',
                        marker_color='#1f77b4'
                    )
                    fig_media_classe.update_layout(
                        yaxis_title="Classe Operacional",
                        xaxis_title="Média de Consumo"
                    )
                    st.plotly_chart(fig_media_classe, use_container_width=True)
                else:
                    st.info("Não há dados de consumo médio para exibir com os filtros e exclusões aplicadas.")
        
        with tab_consulta:
            st.header("🔎 Ficha Individual do Equipamento")
            equip_label = st.selectbox(
                "Selecione o Equipamento", 
                options=df_frotas.sort_values("Cod_Equip")["label"], 
                key="consulta_equip"
            )
        
            if equip_label:
                cod_sel = int(equip_label.split(" - ")[0])
                dados_eq = df_frotas.query("Cod_Equip == @cod_sel").iloc[0]
                consumo_eq = df.query("Cod_Equip == @cod_sel")
                
                st.subheader(f"{dados_eq.get('DESCRICAO_EQUIPAMENTO','–')} ({dados_eq.get('PLACA','–')})")
                
                ultimo_registro = consumo_eq.dropna(subset=['Hod_Hor_Atual']).sort_values("Data", ascending=False).iloc[0] if not consumo_eq.dropna(subset=['Hod_Hor_Atual']).empty else None
                valor_atual_display = formatar_brasileiro_int(ultimo_registro['Hod_Hor_Atual']) if ultimo_registro is not None else "–"
                
                c1, c2, c3 = st.columns(3)
                c1.metric("Status", dados_eq.get("ATIVO", "–"))
                c2.metric("Placa", dados_eq.get("PLACA", "–"))
                c3.metric("Leitura Atual (Hod./Hor.)", valor_atual_display)

                # Indicadores: Checklists/Revisões executadas
                col_filtro_a, col_filtro_b, col_filtro_c = st.columns([1, 1, 2])
                periodo_opcoes = [7, 30, 90, 180, 365]
                periodo_dias = col_filtro_a.selectbox(
                    "Período (dias)", options=periodo_opcoes, index=periodo_opcoes.index(30), key="consulta_periodo_dias"
                )
                filtro_turno = col_filtro_b.selectbox(
                    "Turno (chk)", options=["Todos", "Manhã", "Tarde", "Noite", "N/A"], index=0, key="consulta_turno_chk"
                )
                # Capturar títulos existentes para filtro
                chk_titulos = (
                    sorted(df_checklist_historico['titulo_checklist'].dropna().unique().tolist())
                    if 'titulo_checklist' in df_checklist_historico.columns else []
                )
                filtro_titulo = col_filtro_c.selectbox(
                    "Título do Checklist", options=["Todos"] + chk_titulos, index=0, key="consulta_titulo_chk"
                )
                limite_dt = pd.Timestamp.today().normalize() - pd.Timedelta(days=periodo_dias)
                # Checklists por equipamento
                hist_chk_eq = df_checklist_historico[df_checklist_historico['Cod_Equip'] == cod_sel].copy()
                if not hist_chk_eq.empty and 'data_preenchimento' in hist_chk_eq.columns:
                    hist_chk_eq['data_preenchimento'] = pd.to_datetime(
                        hist_chk_eq['data_preenchimento'], errors='coerce'
                    )
                    if filtro_turno != "Todos" and 'turno' in hist_chk_eq.columns:
                        hist_chk_eq = hist_chk_eq[hist_chk_eq['turno'] == filtro_turno]
                    if filtro_titulo != "Todos" and 'titulo_checklist' in hist_chk_eq.columns:
                        hist_chk_eq = hist_chk_eq[hist_chk_eq['titulo_checklist'] == filtro_titulo]
                chk_total = len(hist_chk_eq)
                chk_30d = (
                    hist_chk_eq[hist_chk_eq['data_preenchimento'] >= limite_dt].shape[0]
                    if 'data_preenchimento' in hist_chk_eq.columns else 0
                )
                # Revisões (manutenções de componentes) por equipamento
                hist_rev_eq = df_comp_historico[df_comp_historico['Cod_Equip'] == cod_sel].copy()
                if not hist_rev_eq.empty and 'Data' in hist_rev_eq.columns:
                    hist_rev_eq['Data'] = pd.to_datetime(hist_rev_eq['Data'], errors='coerce')
                rev_total = len(hist_rev_eq)
                rev_30d = (
                    hist_rev_eq[hist_rev_eq['Data'] >= limite_dt].shape[0] if 'Data' in hist_rev_eq.columns else 0
                )

                m1, m2, m3, m4 = st.columns(4)
                m1.metric(f"Checklists ({periodo_dias}d)", chk_30d)
                m2.metric("Checklists (total)", chk_total)
                m3.metric(f"Revisões ({periodo_dias}d)", rev_30d)
                m4.metric("Revisões (total)", rev_total)
        
                st.markdown("---")

                st.subheader("Comparativo de Eficiência")
            
                col_grafico, col_alerta = st.columns([2, 1]) 

                if 'Media' not in df.columns or df['Media'].dropna().empty:
                    col_grafico.warning("A coluna 'Media' não foi encontrada ou está vazia.")
                else:
                    consumo_real_eq = consumo_eq[(consumo_eq['Media'].notna()) & (consumo_eq['Media'] > 0)]
                    media_equip_selecionado = consumo_real_eq['Media'].mean()
                    
                    classe_selecionada = dados_eq.get('Classe_Operacional')
                    media_da_classe = np.nan

                    if classe_selecionada:
                        consumo_classe = df[(df['Classe_Operacional'] == classe_selecionada) & (df['Media'].notna()) & (df['Media'] > 0)]
                        media_da_classe = consumo_classe['Media'].mean()
                        
                        meta_consumo = st.session_state.intervalos_por_classe.get(classe_selecionada, {}).get('meta_consumo', 0)

                        if pd.notna(media_equip_selecionado) and pd.notna(media_da_classe):
                            with col_alerta:
                                st.write("") 
                                st.write("")
                                if meta_consumo > 0 and media_equip_selecionado > meta_consumo * 1.05:
                                    st.error(f"**ALERTA DE META!** O consumo está acima da meta definida.")
                                elif media_equip_selecionado <= media_da_classe * 1.05:
                                    st.success(f"**EFICIENTE!** O consumo está dentro ou abaixo da média da sua classe.")
                                else:
                                    st.warning(f"**ATENÇÃO!** O consumo está acima da média da classe.")
                                
                                st.metric(label=f"Média do Equipamento", value=formatar_brasileiro(media_equip_selecionado))
                                st.metric(label=f"Média da Classe", value=formatar_brasileiro(media_da_classe))
                                if meta_consumo > 0:
                                    st.metric(label=f"Meta da Classe", value=formatar_brasileiro(meta_consumo))

                            with col_grafico:
                                # --- INÍCIO DA CORREÇÃO ---
                                # 1. Define os novos nomes para as categorias do gráfico
                                nome_frota = f"Frota {dados_eq.get('Cod_Equip')}"
                                nome_classe = f"Média {classe_selecionada}"

                                df_comp = pd.DataFrame({
                                    'Categoria': [nome_frota, nome_classe, "Meta Definida"],
                                    'Média Consumo': [media_equip_selecionado, media_da_classe, meta_consumo]
                                })
                                df_comp['texto_formatado'] = df_comp['Média Consumo'].apply(lambda x: formatar_brasileiro(x))

                                fig_comp = px.bar(
                                    df_comp, 
                                    x='Categoria', 
                                    y='Média Consumo', 
                                    text='texto_formatado', 
                                    title="Eficiência de Consumo vs. Meta",
                                    color='Categoria',
                                    # 2. Atualiza o mapa de cores com os novos nomes
                                    color_discrete_map={
                                        nome_frota: 'royalblue',
                                        nome_classe: 'lightgrey',
                                        'Meta Definida': 'lightcoral'
                                    }
                                )
                                # --- FIM DA CORREÇÃO ---

                                fig_comp.update_traces(textposition='outside', width=0.5)
                                fig_comp.update_layout(height=500, showlegend=False, xaxis_title=None, yaxis_title="Média de Consumo")
                                st.plotly_chart(fig_comp, use_container_width=True)
                        else:
                            col_grafico.info("Não há dados de consumo suficientes para gerar o comparativo.")
                        
                st.markdown("---")
                
                st.markdown("---")
                
                st.subheader("Manutenções Pendentes (Componentes)")
                dados_manut_pendente = plan_df[plan_df['Cod_Equip'] == cod_sel]
                
                if not dados_manut_pendente.empty:
                    componentes_pendentes = []
                    for col in dados_manut_pendente.columns:
                        if 'Restante_' in col:
                            valor_restante = dados_manut_pendente[col].iloc[0]
                            if pd.notna(valor_restante):
                                nome_componente = col.replace('Restante_', '')
                                unidade = dados_manut_pendente['Unidade'].iloc[0]
                                componentes_pendentes.append((nome_componente, valor_restante, unidade))
                    
                    if componentes_pendentes:
                        componentes_pendentes.sort(key=lambda x: x[1]) # Ordena para mostrar os mais urgentes primeiro
                        cols_metricas = st.columns(len(componentes_pendentes))
                        for i, (nome, valor, unid) in enumerate(componentes_pendentes):
                            cols_metricas[i].metric(
                                label=f"Próximo(a) {nome}", 
                                value=f"{formatar_brasileiro_int(valor)} {unid}"
                            )
                    else:
                        st.success("✅ Nenhum componente com manutenção pendente.")
                else:
                    st.info("Sem dados de manutenção para este equipamento.")
                # --- FIM DA MELHORIA 1 ---

                st.markdown("---")
                
                # --- INÍCIO DA MELHORIA 2: Histórico de Manutenção por Componente ---
                st.subheader("Histórico de Manutenções de Componentes")
                # Substitui a tabela de manutenções antigas pela nova
                historico_manut_display = df_comp_historico[df_comp_historico['Cod_Equip'] == cod_sel].sort_values("Data", ascending=False)
                
                if not historico_manut_display.empty:
                    st.dataframe(historico_manut_display[['Data', 'nome_componente', 'Hod_Hor_No_Servico', 'Observacoes']])
                else:
                    st.info("Nenhum histórico de manutenção de componentes para este equipamento.")
                # --- FIM DA MELHORIA 2 ---

                st.subheader("Histórico de Abastecimentos")
                # O seu histórico de abastecimentos continua igual
                historico_abast_display = consumo_eq.sort_values("Data", ascending=False)
                if not historico_abast_display.empty:
                    colunas_abast = ["Data", "Qtde_Litros", "Media", "Hod_Hor_Atual"]
                    st.dataframe(historico_abast_display[[c for c in colunas_abast if c in historico_abast_display]])
                else:
                    st.info("Nenhum registo de abastecimento para este equipamento.")
                            
        with tab_manut:
            st.header("🛠️ Controle de Manutenção")
            
            if not plan_df.empty:
                st.subheader("🚨 Equipamentos com Alertas de Manutenção")
                df_com_alerta = plan_df[plan_df['Qualquer_Alerta'] == True].copy()
                if not df_com_alerta.empty:
                    alert_cols = [col for col in df_com_alerta.columns if 'Alerta_' in col]
                    df_com_alerta['Alertas'] = df_com_alerta[alert_cols].apply(lambda row: ', '.join([col.replace('Alerta_', '') for col, val in row.items() if val is True]), axis=1)
                    display_cols = ['Cod_Equip', 'Equipamento', 'Leitura_Atual', 'Unidade', 'Alertas']

                    df_alertas_display = df_com_alerta[display_cols].copy()
                    df_alertas_display['Leitura_Atual'] = df_alertas_display['Leitura_Atual'].apply(
                        lambda x: formatar_brasileiro_int(x) if pd.notna(x) else ''
                    )
                    st.dataframe(
                        df_alertas_display,
                        column_config={"Cod_Equip": st.column_config.NumberColumn(format="%d")}
                    )

                else:
                    st.success("✅ Nenhum equipamento com alerta no momento.")

                with st.expander("Ver Plano de Manutenção Completo (Quanto Falta)"):
                    cols_to_show = ['Cod_Equip', 'Equipamento', 'Leitura_Atual']
                    for col in plan_df.columns:
                        if 'Restante_' in col and plan_df[col].notna().any():
                            cols_to_show.append(col)
                    
                    df_plano_display = plan_df[cols_to_show].copy()
                    for col in df_plano_display.columns:
                        if col not in ['Cod_Equip', 'Equipamento'] and pd.api.types.is_numeric_dtype(df_plano_display[col]):
                            df_plano_display[col] = df_plano_display[col].apply(
                                lambda x: formatar_brasileiro_int(x) if pd.notna(x) else ''
                            )
                    st.dataframe(
                        df_plano_display,
                        column_config={"Cod_Equip": st.column_config.NumberColumn(format="%d")}
                    )
                    # --- FIM DA CORREÇÃO 2 ---

            else:
                st.info("Não há dados suficientes para gerar o plano de manutenção.")

            st.markdown("---")

        with tab_manut:
            st.header("🛠️ Controle de Manutenção por Componentes")

            if not plan_df.empty:
                df_com_alerta = plan_df[plan_df['Qualquer_Alerta'] == True].copy()
                
                st.subheader("🚨 Equipamentos com Alertas de Manutenção")
                if not df_com_alerta.empty:
                    df_com_alerta['Alertas'] = df_com_alerta['Alertas'].apply(lambda x: ', '.join(x))
                    display_cols = ['Cod_Equip', 'Equipamento', 'Leitura_Atual', 'Unidade', 'Alertas']
                    st.dataframe(df_com_alerta[display_cols])
                else:
                    st.success("✅ Nenhum equipamento com alerta no momento.")

                with st.expander("Ver Plano de Manutenção Completo (Quanto Falta)"):
                    display_cols_full = ['Cod_Equip', 'Equipamento', 'Leitura_Atual']
                    restante_cols = [col for col in plan_df.columns if 'Restante_' in col]
                    st.dataframe(plan_df[display_cols_full + restante_cols])
            else:
                st.info("Não há dados suficientes para gerar o plano de manutenção.")

            st.markdown("---")
            st.subheader("📝 Registrar Manutenção de Componente Realizada")

            with st.form("form_add_comp_service", clear_on_submit=True):
                equip_label = st.selectbox(
                    "Selecione o Equipamento",
                    options=df_frotas.sort_values("label")["label"],
                    key="add_servico_equip"
                )

                componentes_disponiveis = []
                if equip_label:
                    cod_equip_selecionado = int(equip_label.split(" - ")[0])
                    classe_selecionada = df_frotas.loc[df_frotas['Cod_Equip'] == cod_equip_selecionado, 'Classe_Operacional'].iloc[0]
                    regras_classe = df_comp_regras[df_comp_regras['classe_operacional'] == classe_selecionada]
                    if not regras_classe.empty:
                        componentes_disponiveis = regras_classe['nome_componente'].tolist()
                
                componente_servico = st.selectbox("Componente que recebeu manutenção", options=componentes_disponiveis)
                data_servico = st.date_input("Data do Serviço")
                hod_hor_servico = st.number_input("Leitura no Momento do Serviço", min_value=0.0, format="%.2f")
                observacoes = st.text_area("Observações (opcional)")

                if st.form_submit_button("Salvar Manutenção de Componente"):
                    if equip_label and componente_servico:
                        cod_equip = int(equip_label.split(" - ")[0])
                        add_component_service(cod_equip, componente_servico, data_servico.strftime("%Y-%m-%d"), hod_hor_servico, observacoes)
                        st.success(f"Manutenção do componente '{componente_servico}' para '{equip_label}' registrada com sucesso!")
                        rerun_keep_tab("🛠️ Controle de Manutenção")
                    else:
                        st.warning("Por favor, selecione um equipamento e um componente.")
                                    
                    st.markdown("---")
                    st.subheader("📅 Previsão de Próximas Manutenções")
                
            df_previsao = prever_manutencoes(df_frotas, df, plan_df)

            if not df_previsao.empty:
                    # Filtra para mostrar apenas as previsões para os próximos 90 dias
                    st.dataframe(df_previsao[df_previsao['Dias Restantes'] <= 90])
            else:
                    st.info("Não há dados suficientes para gerar uma previsão de manutenções.")
                    
        # APAGUE O CONTEÚDO DA SUA "with tab_checklists:" E SUBSTITUA-O POR ESTE BLOCO

        with tab_checklists:
            st.header("✅ Checklists de Verificação Diária")
            st.info("Esta aba mostra os checklists que, de acordo com as regras, precisam de ser preenchidos hoje.")

            hoje = date.today()
            dia_par = hoje.day % 2 == 0
            
            frotas_a_verificar = df_frotas[df_frotas['ATIVO'] == 'ATIVO']
            regras_a_aplicar = get_checklist_rules()

            if regras_a_aplicar.empty:
                st.warning("Nenhum modelo de checklist foi configurado. Por favor, vá à aba 'Configurações' para criar um.")
            else:
                checklists_para_hoje = False
                for _, regra in regras_a_aplicar.iterrows():
                    regra_aplica_hoje = False
                    # Lógica para determinar se o checklist se aplica hoje
                    if regra['frequencia'] == 'Diário':
                        regra_aplica_hoje = True
                    elif regra['frequencia'] == 'Dias Pares' and dia_par:
                        regra_aplica_hoje = True
                    elif regra['frequencia'] == 'Dias Ímpares' and not dia_par:
                        regra_aplica_hoje = True
                    # Adicionar aqui a lógica de 'Dia Sim/Não' se necessário no futuro

                    if regra_aplica_hoje:
                        checklists_para_hoje = True
                        exp_open_key = st.session_state.get('open_expander_checklists')
                        with st.expander(
                            f"**{regra['titulo_checklist']}** - Turno: {regra['turno']}",
                            expanded=(exp_open_key == f"regra_{regra['id_regra']}"
                        ) ):
                            veiculos_da_classe = frotas_a_verificar[frotas_a_verificar['Classe_Operacional'] == regra['classe_operacional']]
                            itens_checklist = get_checklist_items(regra['id_regra'])

                            if veiculos_da_classe.empty:
                                st.write("Nenhum veículo ativo encontrado para esta classe.")
                                continue
                            if itens_checklist.empty:
                                st.warning("Este checklist não tem itens configurados. Adicione itens na aba 'Configurações'.")
                                continue

                            for _, veiculo in veiculos_da_classe.iterrows():
                                st.subheader(f"Veículo: {veiculo['label']}")
                                
                                ja_preenchido = df_checklist_historico[
                                    (df_checklist_historico['Cod_Equip'] == veiculo['Cod_Equip']) &
                                    (df_checklist_historico['data_preenchimento'] == hoje.strftime('%Y-%m-%d')) &
                                    (df_checklist_historico['turno'] == regra['turno'])
                                ].shape[0] > 0

                                if ja_preenchido:
                                    st.success("✔️ Checklist já preenchido hoje para este turno.")
                                else:
                                    with st.form(f"form_{regra['id_regra']}_{veiculo['Cod_Equip']}", clear_on_submit=True):
                                        status_itens = {}
                                        for _, item in itens_checklist.iterrows():
                                            status_itens[item['nome_item']] = st.selectbox(
                                                item['nome_item'],
                                                options=["Selecione...", "OK", "Com Problema"],
                                                key=f"item_{item['id_item']}_{veiculo['Cod_Equip']}"
                                            )
                                        
                                        if st.form_submit_button("Salvar Checklist"):
                                            if any(v == "Selecione..." for v in status_itens.values()):
                                                st.warning("Selecione uma opção para todos os itens antes de salvar.")
                                            else:
                                                status_geral = "Com Problema" if "Com Problema" in status_itens.values() else "OK"
                                                save_checklist_history(veiculo['Cod_Equip'], regra['titulo_checklist'], hoje.strftime('%Y-%m-%d'), regra['turno'], status_geral)
                                                st.success("Checklist salvo com sucesso!")
                                                st.session_state['open_expander_checklists'] = f"regra_{regra['id_regra']}"
                                                rerun_keep_tab("✅ Checklists Diários")
                
                if not checklists_para_hoje:
                    st.info("Nenhum checklist agendado para hoje.")

                                # bloco duplicado removido
                    
    if st.session_state.role == 'admin':
        with tab_gerir_lanc:
                    st.header("⚙️ Gerir Lançamentos de Abastecimento e Manutenção")
                    acao = st.radio(
                        "Selecione a ação que deseja realizar:",
                        ("Adicionar Abastecimento", "Editar Lançamento", "Excluir Lançamento"),
                        horizontal=True,
                        key="acao_lancamentos"
                    )
                    if acao == "Adicionar Abastecimento":
                        st.subheader("➕ Adicionar Novo Abastecimento")
                        with st.form("form_abastecimento", clear_on_submit=True):
                            equip_selecionado_label = st.selectbox(
                                "Selecione o Equipamento", 
                                options=df_frotas.sort_values("label")["label"],
                                key="add_abast_equip"
                            )
                            data_abastecimento = st.date_input("Data do Abastecimento")
                            qtde_litros = st.number_input("Quantidade de Litros", min_value=0.01, format="%.2f")
                            hod_hor_atual = st.number_input("Hodômetro/Horímetro Atual", min_value=0.01, format="%.2f")
                            safra = st.text_input("Safra (Ex: 2023/2024)")
                    
                            submitted = st.form_submit_button("Salvar Abastecimento")
                    
                            if submitted:
                                if not all([equip_selecionado_label, data_abastecimento, qtde_litros, hod_hor_atual, safra]):
                                    st.warning("Por favor, preencha todos os campos.")
                                else:
                                    cod_equip = int(equip_selecionado_label.split(" - ")[0])
                                    
                                    # --- INÍCIO DA CORREÇÃO ---
                                    # Usa o nome da coluna padronizado ('Classe_Operacional' com underscore)
                                    classe_op = df_frotas.loc[df_frotas['Cod_Equip'] == cod_equip, 'Classe_Operacional'].iloc[0]
                                    # --- FIM DA CORREÇÃO ---
                    
                                    dados_novos = {
                                        'cod_equip': cod_equip,
                                        'data': data_abastecimento.strftime("%Y-%m-%d %H:%M:%S"),
                                        'qtde_litros': qtde_litros,
                                        'hod_hor_atual': hod_hor_atual,
                                        'safra': safra,
                                        'mes': data_abastecimento.month,
                                        'classe_operacional': classe_op
                                    }
                    
                                    if inserir_abastecimento(DB_PATH, dados_novos):
                                        st.success("Abastecimento salvo com sucesso!")
                                        rerun_keep_tab("⚙️ Gerir Lançamentos")
        
                    elif acao == "Excluir Lançamento":
                                st.subheader("🗑️ Excluir um Lançamento")
        
                                df_para_excluir = df.sort_values(by="Data", ascending=False).copy()
                                df_para_excluir['label_exclusao'] = (
                                    df_para_excluir['Data'].dt.strftime('%d/%m/%Y') + " | Frota: " +
                                    df_para_excluir['Cod_Equip'].astype(str) + " - " +
                                    df_para_excluir['DESCRICAO_EQUIPAMENTO'].fillna('N/A') + " | " +
                                    df_para_excluir['Qtde_Litros'].apply(lambda x: f"{x:.2f}".replace('.',',')) + " L | " +
                                    df_para_excluir['Hod_Hor_Atual'].apply(lambda x: formatar_brasileiro_int(x)) + " h/km"
                                )
        
                                # Adiciona um mapeamento de label para rowid para encontrar o registro certo
                                map_label_to_rowid = pd.Series(df_para_excluir.rowid.values, index=df_para_excluir.label_exclusao).to_dict()
        
                                registro_selecionado_label = st.selectbox(
                                    "Selecione o abastecimento a ser excluído (mais recentes primeiro)",
                                    options=df_para_excluir['label_exclusao']
                                )
                                
                                if registro_selecionado_label:
                                    rowid_para_excluir = map_label_to_rowid[registro_selecionado_label]
                                    
                                    st.warning("**Atenção:** Você está prestes a excluir o seguinte registro. Esta ação não pode ser desfeita.")
                                    
                                    # Mostra os detalhes do registro selecionado
                                    registro_detalhes = df[df['rowid'] == rowid_para_excluir]
                                    st.dataframe(registro_detalhes[['Data', 'DESCRICAO_EQUIPAMENTO', 'Qtde_Litros', 'Hod_Hor_Atual']])
        
                                    if st.button("Confirmar Exclusão", type="primary"):
                                        if excluir_abastecimento(DB_PATH, rowid_para_excluir):
                                            st.success("Registro excluído com sucesso!")
                                            rerun_keep_tab("⚙️ Gerir Lançamentos")
                                            
                    elif acao == "Editar Lançamento":
                                st.subheader("✏️ Editar um Lançamento")
                                tipo_edicao = st.radio("O que deseja editar?", ("Abastecimento", "Manutenção"), horizontal=True, key="edit_choice")
        
                                if tipo_edicao == "Abastecimento":
                                    df_abast_edit = df.sort_values(by="Data", ascending=False).copy()
                                    df_abast_edit['label_edit'] = (
                                        df_abast_edit['Data'].dt.strftime('%d/%m/%Y') + " | Frota: " +
                                        df_abast_edit['Cod_Equip'].astype(str) + " - " +
                                        df_abast_edit['DESCRICAO_EQUIPAMENTO'].fillna('N/A') + " | " +
                                        df_abast_edit['Qtde_Litros'].apply(lambda x: f"{x:.2f}".replace('.',',')) + " L | " +
                                        df_abast_edit['Hod_Hor_Atual'].apply(lambda x: formatar_brasileiro_int(x)) + " h/km"
                                    )
                                    map_label_to_rowid = pd.Series(df_abast_edit.rowid.values, index=df_abast_edit.label_edit).to_dict()
                                    label_selecionado = st.selectbox("Selecione o abastecimento para editar", options=df_abast_edit['label_edit'])
                                    
                                    if label_selecionado:
                                        rowid_selecionado = map_label_to_rowid[label_selecionado]
                                        dados_atuais = df[df['rowid'] == rowid_selecionado].iloc[0]
                                    with st.form("form_edit_abastecimento"):
                                        st.write(f"**Editando:** {label_selecionado}")
                                        
                                        # Encontra o índice do equipamento atual para pré-selecionar no selectbox
                                        lista_labels_frotas = df_frotas['label'].tolist()
                                        index_equip_atual = lista_labels_frotas.index(df_frotas[df_frotas['Cod_Equip'] == dados_atuais['Cod_Equip']]['label'].iloc[0])
        
                                        # --- Campos do formulário pré-preenchidos ---
                                        novo_equip_label = st.selectbox(
                                            "Equipamento", 
                                            options=lista_labels_frotas, 
                                            index=index_equip_atual
                                        )
                                        nova_data = st.date_input(
                                            "Data", 
                                            value=pd.to_datetime(dados_atuais['Data']).date()
                                        )
                                        nova_qtde = st.number_input(
                                            "Qtde Litros", 
                                            value=float(dados_atuais['Qtde_Litros']), 
                                            format="%.2f"
                                        )
                                        novo_hod = st.number_input(
                                            "Hod./Hor. Atual", 
                                            value=float(dados_atuais['Hod_Hor_Atual']), 
                                            format="%.2f"
                                        )
                                        nova_safra = st.text_input(
                                            "Safra", 
                                            value=dados_atuais['Safra']
                                        )
        
                                        submitted = st.form_submit_button("Salvar Alterações")
                                        if submitted:
                                            dados_editados = {
                                                'cod_equip': int(novo_equip_label.split(" - ")[0]),
                                                'data': nova_data.strftime("%Y-%m-%d %H:%M:%S"), 
                                                'qtde_litros': nova_qtde,
                                                'hod_hor_atual': novo_hod,
                                                'safra': nova_safra
                                            }
                                            if editar_abastecimento(DB_PATH, rowid_selecionado, dados_editados):
                                                st.success("Abastecimento atualizado com sucesso!")
                                                rerun_keep_tab("⚙️ Gerir Lançamentos")

                                if tipo_edicao == "Manutenção":
                                    st.subheader("Editar Lançamento de Manutenção")

                                    # Garantir que df_manutencoes tenha rowid
                                    if 'rowid' not in df_manutencoes.columns:
                                        df_manutencoes = df_manutencoes.reset_index().rename(columns={'index': 'rowid'})

                                    # Usa o df_manutencoes original (preserva rowid)
                                    df_manut_edit = df_manutencoes.copy()

                                    # Garante que a coluna Data seja datetime
                                    df_manut_edit['Data'] = pd.to_datetime(df_manut_edit['Data'], errors='coerce')

                                    # Remove duplicatas de Cod_Equip no df_frotas para evitar erro no map
                                    df_frotas_unique = df_frotas.drop_duplicates(subset=['Cod_Equip'], keep='first')

                                    # Adiciona descrição do equipamento via map
                                    desc_map = df_frotas_unique.set_index('Cod_Equip')['DESCRICAO_EQUIPAMENTO']
                                    df_manut_edit['DESCRICAO_EQUIPAMENTO'] = df_manut_edit['Cod_Equip'].map(desc_map).fillna('N/A')

                                    # Garantir que df_manut_edit tenha rowid
                                    if 'rowid' not in df_manut_edit.columns:
                                        if 'rowid_frota' in df_manut_edit.columns:
                                            df_manut_edit.rename(columns={'rowid_frota': 'rowid'}, inplace=True)
                                        else:
                                            df_manut_edit.reset_index(inplace=True)
                                            df_manut_edit.rename(columns={'index': 'rowid'}, inplace=True)

                                    # Ordena e cria os labels para seleção
                                    df_manut_edit.sort_values(by="Data", ascending=False, inplace=True)
                                    df_manut_edit['label_edit'] = (
                                        df_manut_edit['Data'].dt.strftime('%d/%m/%Y') + " | Frota: " +
                                        df_manut_edit['Cod_Equip'].astype(str) + " - " +
                                        df_manut_edit['DESCRICAO_EQUIPAMENTO'].fillna('N/A') + " | " +
                                        df_manut_edit['Tipo_Servico'] + " | " +
                                        df_manut_edit['Hod_Hor_No_Servico'].apply(lambda x: formatar_brasileiro_int(x)) + " h/km"
                                    )

                                    # Cria o dicionário de label -> rowid
                                    map_label_to_rowid = pd.Series(
                                        df_manut_edit['rowid'].values,
                                        index=df_manut_edit['label_edit']
                                    ).to_dict()

                                    # Selectbox para escolher manutenção
                                    label_selecionado = st.selectbox(
                                        "Selecione a manutenção para editar",
                                        options=df_manut_edit['label_edit'],
                                        key="manut_edit_select"
                                    )

                                    if label_selecionado:
                                        rowid_selecionado = map_label_to_rowid.get(label_selecionado)
                                        if rowid_selecionado is not None:
                                            dados_atuais = df_manutencoes[df_manutencoes['rowid'] == rowid_selecionado].iloc[0]

                                            with st.form("form_edit_manutencao"):
                                                st.write(f"**Editando:** {label_selecionado}")

                                                lista_labels_frotas = df_frotas.sort_values("label")['label'].tolist()
                                                equip_atual = df_frotas[df_frotas['Cod_Equip'] == dados_atuais['Cod_Equip']]['label'].iloc[0]
                                                index_equip_atual = lista_labels_frotas.index(equip_atual)

                                                novo_equip_label = st.selectbox("Equipamento", options=lista_labels_frotas, index=index_equip_atual)

                                                classe_selecionada = df_frotas[df_frotas['label'] == novo_equip_label]['Classe_Operacional'].iloc[0]
                                                servicos_configurados = st.session_state.intervalos_por_classe.get(classe_selecionada, {}).get('servicos', {})
                                                servicos_disponiveis = [info['nome'] for info in servicos_configurados.values()]

                                                index_servico_atual = servicos_disponiveis.index(dados_atuais['Tipo_Servico']) if dados_atuais['Tipo_Servico'] in servicos_disponiveis else 0

                                                novo_tipo_servico = st.selectbox("Tipo de Serviço", options=servicos_disponiveis, index=index_servico_atual)
                                                nova_data = st.date_input("Data", value=pd.to_datetime(dados_atuais['Data']).date())
                                                novo_hod = st.number_input("Hod./Hor. no Serviço", value=float(dados_atuais['Hod_Hor_No_Servico']), format="%.2f")

                                                submitted = st.form_submit_button("Salvar Alterações")
                                                if submitted:
                                                    dados_editados = {
                                                        'cod_equip': int(novo_equip_label.split(" - ")[0]),
                                                        'data': nova_data.strftime("%Y-%m-%d"),
                                                        'tipo_servico': novo_tipo_servico,
                                                        'hod_hor_servico': novo_hod,
                                                    }
                                                    if editar_manutencao(DB_PATH, rowid_selecionado, dados_editados):
                                                        st.success("Manutenção atualizada com sucesso!")
                                                        rerun_keep_tab("⚙️ Gerir Lançamentos")


        with tab_gerir_frotas:
                st.header("⚙️ Gerir Frotas")
                acao_frota = st.radio(
                    "Selecione a ação que deseja realizar:",
                    ("Cadastrar Nova Frota", "Editar Frota Existente"),
                    horizontal=True,
                    key="acao_frotas"
                )
        
                if acao_frota == "Cadastrar Nova Frota":
                        st.subheader("➕ Cadastrar Nova Frota")
                        with st.form("form_nova_frota", clear_on_submit=True):
                                st.info("Certifique-se de que o Código do Equipamento é único e não existe na base de dados.")
                                
                                # Campos do formulário
                                cod_equip = st.number_input("Código do Equipamento (único)", min_value=1, step=1)
                                descricao = st.text_input("Descrição do Equipamento (ex: CAMINHÃO BASCULANTE)")
                                placa = st.text_input("Placa (deixe em branco se não aplicável)")
                                classe_op = st.text_input("Classe Operacional (ex: Caminhões Pesados)")
                                ativo = st.selectbox("Status", options=["ATIVO", "INATIVO"])
                                
                                submitted_frota = st.form_submit_button("Salvar Novo Equipamento")
                                
                                if submitted_frota:
                                    # Validação
                                    if not all([cod_equip, descricao, classe_op]):
                                        st.warning("Os campos 'Código', 'Descrição' e 'Classe Operacional' são obrigatórios.")
                                    elif cod_equip in df_frotas['Cod_Equip'].values:
                                        st.error(f"Erro: O Código de Equipamento '{cod_equip}' já existe! Por favor, escolha outro.")
                                    else:
                                        # Prepara os dados para inserção
                                        dados_frota = {
                                            'cod_equip': cod_equip,
                                            'descricao': descricao,
                                            'placa': placa if placa else None, # Salva None se o campo estiver vazio
                                            'classe_op': classe_op,
                                            'ativo': ativo
                                        }
                                        
                                        if inserir_frota(DB_PATH, dados_frota):
                                            st.success(f"Equipamento '{descricao}' cadastrado com sucesso!")
                                            rerun_keep_tab("⚙️ Gerir Frotas")
            
                elif acao_frota == "Editar Frota Existente":
                    st.subheader("✏️ Editar Frota Existente")
                    equip_para_editar_label = st.selectbox(
                        "Selecione o equipamento que deseja editar",
                        options=df_frotas.sort_values("label")["label"],
                        key="frota_edit_select"
                    )
        
                    if equip_para_editar_label:
                        cod_equip_edit = int(equip_para_editar_label.split(" - ")[0])
                        dados_atuais = df_frotas[df_frotas['Cod_Equip'] == cod_equip_edit].iloc[0]
        
                        with st.form("form_edit_frota"):
                            st.write(f"**Editando:** {dados_atuais['DESCRICAO_EQUIPAMENTO']} (Cód: {dados_atuais['Cod_Equip']})")
        
                            nova_descricao = st.text_input("Descrição do Equipamento", value=dados_atuais['DESCRICAO_EQUIPAMENTO'])
                            nova_placa = st.text_input("Placa", value=dados_atuais['PLACA'])
                            nova_classe_op = st.text_input("Classe Operacional", value=dados_atuais['Classe_Operacional'])
                            
                            status_options = ["ATIVO", "INATIVO"]
                            index_status = status_options.index(dados_atuais['ATIVO']) if dados_atuais['ATIVO'] in status_options else 0
                            novo_status = st.selectbox("Status", options=status_options, index=index_status)
        
                            submitted = st.form_submit_button("Salvar Alterações na Frota")
                            if submitted:
                                dados_editados = {
                                    'descricao': nova_descricao,
                                    'placa': nova_placa,
                                    'classe_op': nova_classe_op,
                                    'ativo': novo_status
                                }
                                if editar_frota(DB_PATH, cod_equip_edit, dados_editados):
                                    st.success("Dados da frota atualizados com sucesso!")
                                    rerun_keep_tab("⚙️ Gerir Frotas")
                    

        # APAGUE O CONTEÚDO DA SUA "with tab_config:" E SUBSTITUA-O POR ESTE BLOCO

        with tab_config:
            st.header("⚙️ Configurar Manutenções e Checklists")
            
            # --- Gestão de Componentes ---
            exp_comp_open = st.session_state.get('open_expander_config_componentes', False)
            with st.expander("Configurar Componentes de Manutenção por Classe", expanded=bool(exp_comp_open)):
                classes_operacionais = sorted([c for c in df_frotas['Classe_Operacional'].unique() if pd.notna(c) and str(c).strip()])
                df_comp_regras = get_component_rules() # Busca os dados mais recentes

                for classe in classes_operacionais:
                    with st.container():
                        st.subheader(f"Classe: {classe}")
                        regras_atuais = df_comp_regras[df_comp_regras['classe_operacional'] == classe]
                        
                        # Exibe as regras atuais com um botão para apagar
                        for _, regra in regras_atuais.iterrows():
                            col1, col2, col3 = st.columns([2, 1, 1])
                            col1.write(regra['nome_componente'])
                            col2.write(f"{regra['intervalo_padrao']} { 'km' if df_frotas[df_frotas['Classe_Operacional'] == classe]['Tipo_Controle'].iloc[0] == 'QUILÔMETROS' else 'h' }")
                            if col3.button("Remover", key=f"del_comp_{regra['id_regra']}"):
                                delete_component_rule(regra['id_regra'])
                                rerun_keep_tab("⚙️ Configurações")

                        with st.form(f"form_add_{classe}", clear_on_submit=True):
                            st.write("**Adicionar Novo Componente**")
                            novo_comp_nome = st.text_input("Nome do Componente", key=f"nome_{classe}")
                            novo_comp_intervalo = st.number_input("Intervalo", min_value=1, step=50, key=f"int_{classe}")
                            if st.form_submit_button("Adicionar Componente"):
                                add_component_rule(classe, novo_comp_nome, novo_comp_intervalo)
                                st.session_state['open_expander_config_componentes'] = True
                                rerun_keep_tab("⚙️ Configurações")
                        st.markdown("---")

            # --- Gestão de Checklists ---
            exp_chk_open = st.session_state.get('open_expander_config_checklists', True)
            with st.expander("Configurar Checklists Diários", expanded=bool(exp_chk_open)):
                st.subheader("Modelos de Checklist Existentes")
                regras_checklist = get_checklist_rules()
                if not regras_checklist.empty:
                    st.table(regras_checklist[['titulo_checklist', 'classe_operacional', 'frequencia', 'turno']])
                else:
                    st.info("Nenhum modelo de checklist criado.")

                with st.form("form_add_checklist", clear_on_submit=True):
                    st.subheader("Criar Novo Modelo de Checklist")
                    
                    col1_form, col2_form = st.columns(2)
                    nova_classe = col1_form.selectbox("Aplicar à Classe Operacional", options=classes_operacionais, key="chk_classe")
                    novo_titulo = col1_form.text_input("Título do Checklist (ex: Verificação Matinal Colhedoras)", key="chk_titulo")
                    nova_frequencia = col2_form.selectbox("Frequência", options=['Diário', 'Dias Pares', 'Dias Ímpares'], key="chk_freq")
                    novo_turno = col2_form.selectbox("Turno", options=['Manhã', 'Noite', 'N/A'], key="chk_turno")
                    
                    st.write("**Itens a serem verificados (um por linha):**")
                    novos_itens_texto = st.text_area("Itens do Checklist", height=150, key="chk_itens", placeholder="Nível do Óleo\nPressão dos Pneus\nVerificar Facas")
                    
                    if st.form_submit_button("Salvar Novo Modelo de Checklist"):
                        if nova_classe and novo_titulo and novos_itens_texto:
                            # Ordem correta dos parâmetros: (classe, título, turno, frequência)
                            rule_id = add_checklist_rule_and_get_id(nova_classe, novo_titulo, novo_turno, nova_frequencia)
                            if rule_id is None:
                                st.error("Não foi possível criar a regra do checklist.")
                            else:
                                itens_lista = [item.strip() for item in novos_itens_texto.split('\n') if item.strip()]
                                for item in itens_lista:
                                    add_checklist_item(rule_id, item)
                                st.success("Novo modelo de checklist criado com sucesso!")
                                st.session_state['open_expander_config_checklists'] = True
                                rerun_keep_tab("⚙️ Configurações")
                        else:
                            st.warning("Por favor, preencha todos os campos obrigatórios.")
                        
        with tab_importar:
                    st.header("📤 Importar Novos Abastecimentos de uma Planilha")
                    st.info("Esta funcionalidade permite carregar múltiplos abastecimentos de uma vez a partir de um arquivo Excel (.xlsx).")
                    st.warning("**Atenção:** Certifique-se de que a sua planilha contém as seguintes colunas: `Cód. Equip.`, `Data`, `Qtde Litros`, `Hod. Hor. Atual`, `Safra`, `Mês`, `Classe Operacional`.")
            
                    arquivo_carregado = st.file_uploader(
                        "Selecione a sua planilha de abastecimentos",
                        type=['xlsx']
                    )
            
                    if arquivo_carregado is not None:
                        st.markdown("---")
                        st.write("**Pré-visualização dos dados a serem importados:**")
                        
                        try:
                            df_preview = pd.read_excel(arquivo_carregado)
                            st.dataframe(df_preview.head())
            
                            if st.button("Confirmar e Inserir Dados no Banco de Dados", type="primary"):
                                with st.spinner("Importando dados... por favor, aguarde."):
                                    num_inseridos, num_duplicados, mensagem = importar_abastecimentos_de_planilha(DB_PATH, arquivo_carregado)
                                
                                if num_inseridos > 0:
                                    msg_sucesso = f"**Sucesso!** {num_inseridos} registos foram importados."
                                    if num_duplicados > 0:
                                        msg_sucesso += f" {num_duplicados} registos duplicados foram ignorados."
                                    st.success(msg_sucesso + " O dashboard será atualizado.")
                                    rerun_keep_tab("📤 Importar Dados")
                                else:
                                    st.error(mensagem)
                        except Exception as e:
                            st.error(f"Não foi possível ler a planilha. Verifique se o arquivo está no formato correto. Detalhes do erro: {e}")
                            
        with tab_saude:
                    st.header("⚕️ Painel de Controlo da Qualidade dos Dados")
                    st.info("Esta sessão verifica automaticamente a sua base de dados em busca de erros comuns.")

                    st.subheader("1. Verificação de Leituras de Hodómetro/Horímetro")
                    df_abastecimentos_sorted = df.sort_values(by=['Cod_Equip', 'Data'])
                    df_abastecimentos_sorted['Leitura_Anterior'] = df_abastecimentos_sorted.groupby('Cod_Equip')['Hod_Hor_Atual'].shift(1)

                    erros_hodometro = df_abastecimentos_sorted[
                        df_abastecimentos_sorted['Hod_Hor_Atual'] < df_abastecimentos_sorted['Leitura_Anterior']
                    ]

                    if not erros_hodometro.empty:
                        st.error(f"**Alerta:** Foram encontrados {len(erros_hodometro)} lançamentos com leituras de hodómetro/horímetro menores que a anterior.")
                        st.dataframe(erros_hodometro[['Data', 'Cod_Equip', 'DESCRICAO_EQUIPAMENTO', 'Hod_Hor_Atual', 'Leitura_Anterior']])
                    else:
                        st.success("✅ Nenhuma inconsistência encontrada nas leituras de hodómetro/horímetro.")

                    st.markdown("---")
                    st.subheader("2. Verificação de Frotas Inativas")

                    data_limite = datetime.now() - pd.Timedelta(days=90)
                    ultimos_abastecimentos = df.groupby('Cod_Equip')['Data'].max()

                    frotas_ativas = df_frotas[df_frotas['ATIVO'] == 'ATIVO'].copy()
                    frotas_ativas['Ultimo_Abastecimento'] = frotas_ativas['Cod_Equip'].map(ultimos_abastecimentos)

                    frotas_inativas = frotas_ativas[
                        (frotas_ativas['Ultimo_Abastecimento'].isna()) | 
                        (frotas_ativas['Ultimo_Abastecimento'] < data_limite)
                    ]

                    if not frotas_inativas.empty:
                        st.warning(f"**Atenção:** Foram encontradas {len(frotas_inativas)} frotas marcadas como 'ATIVAS' que não têm abastecimentos nos últimos 90 dias.")
                        st.dataframe(frotas_inativas[['Cod_Equip', 'DESCRICAO_EQUIPAMENTO', 'Ultimo_Abastecimento']])
                    else:
                        st.success("✅ Todas as frotas ativas têm registos de abastecimento recentes.")
                        
        with tab_gerir_users:
                        st.header("👤 Gestão de Usuários")
                        
                        acao_user = st.radio(
                            "Selecione uma ação:",
                            ("Adicionar Novo Usuário", "Editar Usuário", "Remover Usuário"),
                            horizontal=True
                        )

                        st.markdown("---")
                        df_users = get_all_users()

                        if acao_user == "Adicionar Novo Usuário":
                            with st.form("form_add_user", clear_on_submit=True):
                                st.subheader("Adicionar Novo Usuário")
                                novo_user = st.text_input("Nome de Usuário")
                                nova_pass = st.text_input("Senha", type="password")
                                novo_role = st.selectbox("Função (Role)", ("admin", "básico"))
                                
                                if st.form_submit_button("Adicionar Usuário"):
                                    success, message = add_user(novo_user, nova_pass, novo_role)
                                    if success:
                                        st.success(message)
                                        rerun_keep_tab("👤 Gerir Utilizadores", clear_cache=False)
                                    else:
                                        st.error(message)
                        
                        elif acao_user == "Editar Usuário":
                            st.subheader("Editar Usuário Existente")
                            user_a_editar = st.selectbox("Selecione o usuário para editar", options=df_users['username'])
                            
                            if user_a_editar:
                                user_data = df_users[df_users['username'] == user_a_editar].iloc[0]
                                
                                with st.form("form_edit_user"):
                                    st.write(f"A editar: **{user_data['username']}**")
                                    novo_username = st.text_input("Novo nome de usuário", value=user_data['username'])
                                    
                                    roles = ["admin", "básico"]
                                    role_index = roles.index(user_data['role']) if user_data['role'] in roles else 0
                                    novo_role_edit = st.selectbox("Novo cargo", options=roles, index=role_index)
                                    
                                    if st.form_submit_button("Salvar Alterações"):
                                        success, message = update_user(user_data['id'], novo_username, novo_role_edit)
                                        if success:
                                            st.success(message)
                                            rerun_keep_tab("👤 Gerir Utilizadores", clear_cache=False)
                                        else:
                                            st.error(message)

                        elif acao_user == "Remover Usuário":
                            st.subheader("Remover Usuário")
                            user_a_remover = st.selectbox("Selecione o usuário para remover", options=df_users['username'])
                            
                            if user_a_remover:
                                if user_a_remover == st.session_state.username:
                                    st.error("Não pode remover o seu próprio usuário enquanto estiver com a sessão iniciada.")
                                else:
                                    st.warning(f"**Atenção:** Tem a certeza de que deseja remover o usuário **{user_a_remover}**? Esta ação é irreversível.")
                                    if st.button("Confirmar Remoção", type="primary"):
                                        user_id_remover = df_users[df_users['username'] == user_a_remover].iloc[0]['id']
                                        success, message = delete_user(user_id_remover)
                                        if success:
                                            st.success(message)
                                            rerun_keep_tab("👤 Gerir Utilizadores", clear_cache=False)
                                        else:
                                            st.error(message)
        with tab_gerir_checklists:
            st.header("✅ Gerir Checklists")
            
            # Criar abas para organizar melhor as funcionalidades
            tab_config, tab_historico = st.tabs(["⚙️ Configuração", "🗑️ Histórico"])
            
            with tab_config:
                col_regras, col_itens = st.columns(2)
                with col_regras:

                    st.subheader("📋 Regras de Checklist")
                    regras_df = get_checklist_rules()
                    if not regras_df.empty:
                        st.dataframe(regras_df)
                    else:
                        st.info("Nenhuma regra cadastrada.")

                    with st.form("form_add_regra", clear_on_submit=True):
                        id_regra_edit = st.selectbox(
                            "Editar Regra (ou deixe em branco para criar nova)",
                            options=[""] + (regras_df['id_regra'].astype(str).tolist() if not regras_df.empty else [""])
                        )
                        classe_op = st.text_input("Classe Operacional")
                        titulo = st.text_input("Título do Checklist")
                        frequencia = st.selectbox("Frequência", ["Diário", "Dias Pares", "Dias Ímpares"])
                        turno = st.selectbox("Turno", ["Manhã", "Tarde", "Noite"]) 

                        if st.form_submit_button("Salvar Regra"):
                            if id_regra_edit:
                                ok, msg = edit_checklist_rule(int(id_regra_edit), classe_op, titulo, turno, frequencia)
                            else:
                                ok, msg = add_checklist_rule(classe_op, titulo, turno, frequencia)
                            if ok:
                                st.success(str(msg))
                            else:
                                st.error(str(msg))
                            rerun_keep_tab("✅ Gerir Checklists")

                    if not regras_df.empty:
                        regra_del = st.selectbox("Selecione a Regra para excluir", regras_df['id_regra'])
                        if st.button("Excluir Regra"):
                            ok, msg = delete_checklist_rule(regra_del)
                            if ok:
                                st.success(str(msg))
                            else:
                                st.error(str(msg))
                            rerun_keep_tab("✅ Gerir Checklists")

                with col_itens:
                    st.subheader("📝 Itens de Checklist")
                    if regras_df.empty:
                        st.warning("Cadastre pelo menos uma regra para poder adicionar itens.")
                    else:
                        regra_sel = st.selectbox("Selecione uma Regra para gerenciar itens", regras_df['id_regra'])
                        itens_df = get_checklist_items(regra_sel)
                        if not itens_df.empty:
                            st.dataframe(itens_df)
                        else:
                            st.info("Nenhum item para esta regra.")

                        with st.form("form_add_item", clear_on_submit=True):
                            id_item_edit = st.selectbox(
                                "Editar Item (ou deixe em branco para criar novo)",
                                options=[""] + itens_df['id_item'].astype(str).tolist() if not itens_df.empty else [""]
                            )
                            nome_item = st.text_input("Nome do Item")
                            if st.form_submit_button("Salvar Item"):
                                if id_item_edit:
                                    ok, msg = edit_checklist_item(int(id_item_edit), nome_item)
                                else:
                                    ok, msg = add_checklist_item(regra_sel, nome_item)
                                if ok:
                                    st.success(str(msg))
                                else:
                                    st.error(str(msg))
                                rerun_keep_tab("✅ Gerir Checklists")

                        if not itens_df.empty:
                            item_del = st.selectbox("Selecione o Item para excluir", itens_df['id_item'])
                            if st.button("Excluir Item"):
                                ok, msg = delete_checklist_item(item_del)
                                if ok:
                                    st.success(str(msg))
                                else:
                                    st.error(str(msg))
                                rerun_keep_tab("✅ Gerir Checklists")
            
            with tab_historico:
                st.subheader("🗑️ Excluir Checklists Lançados")
                st.info("Esta seção permite excluir checklists que já foram preenchidos e salvos no histórico.")
                
                # Carregar dados do histórico
                df_historico = df_checklist_historico.copy()
                
                if df_historico.empty:
                    st.warning("Nenhum checklist foi preenchido ainda.")
                else:
                    # Adicionar informações do equipamento ao histórico
                    df_historico = df_historico.merge(
                        df_frotas[['Cod_Equip', 'DESCRICAO_EQUIPAMENTO']], 
                        on='Cod_Equip', 
                        how='left'
                    )
                    
                    # Criar labels para seleção
                    df_historico['label_exclusao'] = (
                        df_historico['data_preenchimento'] + " | " +
                        df_historico['Cod_Equip'].astype(str) + " - " +
                        df_historico['DESCRICAO_EQUIPAMENTO'].fillna('N/A') + " | " +
                        df_historico['titulo_checklist'] + " | " +
                        df_historico['turno'] + " | " +
                        "Status: " + df_historico['status_geral']
                    )
                    
                    # Ordenar por data mais recente
                    df_historico = df_historico.sort_values(by='data_preenchimento', ascending=False)
                    
                    # Seleção do checklist para excluir
                    checklist_selecionado = st.selectbox(
                        "Selecione o checklist para excluir:",
                        options=df_historico['label_exclusao'],
                        key="checklist_exclusao"
                    )
                    
                    if checklist_selecionado:
                        # Encontrar os detalhes do checklist selecionado
                        checklist_detalhes = df_historico[df_historico['label_exclusao'] == checklist_selecionado].iloc[0]
                        
                        # Mostrar detalhes do checklist selecionado
                        st.warning("**Atenção:** Você está prestes a excluir o seguinte checklist. Esta ação não pode ser desfeita.")
                        
                        col1, col2 = st.columns(2)
                        with col1:
                            st.write(f"**Data:** {checklist_detalhes['data_preenchimento']}")
                            st.write(f"**Equipamento:** {checklist_detalhes['Cod_Equip']} - {checklist_detalhes['DESCRICAO_EQUIPAMENTO']}")
                        with col2:
                            st.write(f"**Título:** {checklist_detalhes['titulo_checklist']}")
                            st.write(f"**Turno:** {checklist_detalhes['turno']}")
                            st.write(f"**Status:** {checklist_detalhes['status_geral']}")
                        
                        # Botão de confirmação
                        if st.button("🗑️ Confirmar Exclusão", type="primary"):
                            success, message = delete_checklist_history(
                                checklist_detalhes['Cod_Equip'],
                                checklist_detalhes['titulo_checklist'],
                                checklist_detalhes['data_preenchimento'],
                                checklist_detalhes['turno']
                            )
                            if success:
                                st.success(message)
                                rerun_keep_tab("✅ Gerir Checklists")
                            else:
                                st.error(message)

                    
if __name__ == "__main__":
    main()
