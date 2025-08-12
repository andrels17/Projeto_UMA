import streamlit as st
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime
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

@st.cache_data
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

@st.cache_data(show_spinner="Carregando e processando dados...")
def load_data_from_db(db_path: str) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    if not os.path.exists(db_path):
        st.error(f"Arquivo de banco de dados '{db_path}' não encontrado.")
        st.stop()
    try:
        with sqlite3.connect(db_path, check_same_thread=False) as conn:
            df_abast = pd.read_sql_query("SELECT rowid, * FROM abastecimentos", conn)
            df_frotas = pd.read_sql_query("SELECT * FROM frotas", conn)
            df_manutencoes = pd.read_sql_query("SELECT rowid, * FROM manutencoes", conn)
    except Exception as e:
        if "no such table" in str(e):
            st.error(f"Erro de banco de dados: Tabela não encontrada. Detalhes: {e}")
            st.stop()
        else:
            st.error(f"Erro ao ler o banco de dados: {e}")
            st.stop()

    df_abast = df_abast.rename(columns={"Cód. Equip.": "Cod_Equip", "Qtde Litros": "Qtde_Litros", "Mês": "Mes", "Média": "Media"}, errors='ignore')
    df_frotas = df_frotas.rename(columns={"COD_EQUIPAMENTO": "Cod_Equip"}, errors='ignore')
    
    df = pd.merge(df_abast, df_frotas, on="Cod_Equip", how="left")
    
    if 'Classe Operacional_x' in df.columns:
        df['Classe_Operacional'] = np.where(df['Classe Operacional_x'].notna(), df['Classe Operacional_x'], df['Classe Operacional_y'])
        df.drop(columns=['Classe Operacional_x', 'Classe Operacional_y'], inplace=True)
    elif 'Classe Operacional' in df.columns:
        df.rename(columns={'Classe Operacional': 'Classe_Operacional'}, inplace=True)
        
    df["Data"] = pd.to_datetime(df["Data"], errors='coerce')
    df.dropna(subset=["Data"], inplace=True)
    df["Ano"] = df["Data"].dt.year
    df["AnoMes"] = df["Data"].dt.to_period("M").astype(str)
    
    for col in ["Qtde_Litros", "Media", "Hod_Hor_Atual"]:
        if col in df.columns:
            series = df[col].astype(str)
            series = series.str.replace(',', '.', regex=False).str.replace('-', '', regex=False).str.strip()
            df[col] = pd.to_numeric(series, errors='coerce')

    df_frotas["label"] = df_frotas["Cod_Equip"].astype(str) + " - " + df_frotas.get("DESCRICAO_EQUIPAMENTO", "").fillna("") + " (" + df_frotas.get("PLACA", "").fillna("Sem Placa") + ")"
    
    classe_map = df.dropna(subset=['Classe_Operacional']).groupby('Cod_Equip')['Classe_Operacional'].first()
    df_frotas['Classe_Operacional'] = df_frotas['Cod_Equip'].map(classe_map).fillna(df_frotas.get('Classe Operacional'))
    
    def determinar_tipo_controle(row):
        texto_para_verificar = (str(row.get('DESCRICAO_EQUIPAMENTO', '')) + ' ' + str(row.get('Classe_Operacional', ''))).upper()
        km_keywords = ['CAMINH', 'VEICULO', 'PICKUP', 'CAVALO MECANICO']
        if any(p in texto_para_verificar for p in km_keywords):
            return 'QUILÔMETROS'
        return 'HORAS'
    df_frotas['Tipo_Controle'] = df_frotas.apply(determinar_tipo_controle, axis=1)

    return df, df_frotas, df_manutencoes
    
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
            "Hod. Hor. Atual": "Hod. Hor. Atual",
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

@st.cache_data
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

# SUBSTITUA A SUA FUNÇÃO "build_maintenance_plan" POR ESTE BLOCO
@st.cache_data(show_spinner="Calculando plano de manutenção...")
def build_maintenance_plan(_df_frotas: pd.DataFrame, _df_abastecimentos: pd.DataFrame, _df_manutencoes: pd.DataFrame, intervalos_por_classe: dict) -> pd.DataFrame:
    latest_readings = _df_abastecimentos.sort_values('Data').groupby('Cod_Equip')['Hod_Hor_Atual'].last()
    plan_data = []
    for _, frota_row in _df_frotas.iterrows():
        cod_equip = frota_row['Cod_Equip']
        tipo_controle = frota_row['Tipo_Controle']
        classe_op = frota_row.get('Classe_Operacional')
        hod_hor_atual = latest_readings.get(cod_equip)
        if pd.isna(hod_hor_atual) or not classe_op: continue
        
        config_classe = intervalos_por_classe.get(classe_op, {})
        servicos_aplicaveis = config_classe.get('servicos', {})
        if not servicos_aplicaveis: continue
        
        unidade = 'km' if tipo_controle == 'QUILÔMETROS' else 'h'
        alerta_default = ALERTAS_MANUTENCAO.get(tipo_controle, {}).get('default', 500)
        record = {'Cod_Equip': cod_equip, 'Equipamento': frota_row.get('DESCRICAO_EQUIPAMENTO', 'N/A'), 'Leitura_Atual': hod_hor_atual, 'Unidade': unidade, 'Qualquer_Alerta': False}
        
        for servico_id, servico_info in servicos_aplicaveis.items():
            nome_servico = servico_info.get('nome', servico_id)
            intervalo = servico_info.get('intervalo', 0)
            if not intervalo or intervalo <= 0: continue
            
            manutencoes_servico = _df_manutencoes[(_df_manutencoes['Cod_Equip'] == cod_equip) & (_df_manutencoes['Tipo_Servico'] == nome_servico)]
            ultimo_servico_hod_hor = 0
            if not manutencoes_servico.empty: ultimo_servico_hod_hor = manutencoes_servico['Hod_Hor_No_Servico'].max()
            
            base_calculo = ultimo_servico_hod_hor if hod_hor_atual >= ultimo_servico_hod_hor else hod_hor_atual
            multiplicador = (hod_hor_atual - base_calculo) // intervalo
            prox_servico = base_calculo + (multiplicador + 1) * intervalo
            restante = prox_servico - hod_hor_atual
            alerta = restante <= alerta_default
            
            if alerta: record['Qualquer_Alerta'] = True
            record[f'Prox_{nome_servico}'] = prox_servico
            record[f'Restante_{nome_servico}'] = restante
            record[f'Alerta_{nome_servico}'] = alerta
        plan_data.append(record)
        
    if not plan_data: return pd.DataFrame()
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

def main():
    st.set_page_config(page_title="Dashboard de Frotas", layout="wide")
    
    if 'authenticated' not in st.session_state:
        st.session_state.authenticated = False
        st.session_state.role = None
        st.session_state.username = ""

    if not st.session_state.authenticated:
        st.title("Login - Dashboard de Frotas")
        username = st.text_input("Usuário")
        password = st.text_input("Login", type="password")
        
        if st.button("Entrar"):
            role = check_login_db(username, password) # <-- CORREÇÃO APLICADA
            if role:
                st.session_state.authenticated = True
                st.session_state.role = role
                st.session_state.username = username # Preenche o username após o login
                st.rerun()
            else:
                st.error("Usuário ou senha incorretos.")
    else:
    
        st.title("📊 Dashboard de Frotas e Abastecimentos")

        df, df_frotas, df_manutencoes = load_data_from_db(DB_PATH)

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
            st.write(f"Bem-vindo, **{st.session_state.username}**!") # Agora esta linha funciona
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
        plan_df = build_maintenance_plan(df_frotas, df, df_manutencoes, st.session_state.intervalos_por_classe)

        abas_visualizacao = ["📊 Painel de Controlo", "📈 Análise Geral", "🛠️ Controle de Manutenção", "🔎 Consulta Individual"]
        abas_admin = ["⚙️ Gerir Lançamentos", "⚙️ Gerir Frotas", "📤 Importar Dados", "⚙️ Configurações", "⚕️ Saúde dos Dados", "👤 Gerir Utilizadores"]
            
        if st.session_state.role == 'admin':
            tabs_para_mostrar = abas_visualizacao + abas_admin
            abas = st.tabs(tabs_para_mostrar)
            # Desempacota todas as abas
            (tab_painel, tab_analise, tab_manut, tab_consulta, 
             tab_gerir_lanc, tab_gerir_frotas, tab_importar, tab_config, tab_saude, 
             tab_gerir_users) = abas
            
        else:
            tabs_para_mostrar = abas_visualizacao
            tab_painel, tab_analise, tab_manut, tab_consulta = st.tabs(tabs_para_mostrar)
                
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
                            
                            def formatar_eficiencia(val):
                                if pd.isna(val): return "N/A"
                                if val > 5: return f"🟢 {val:+.2f}%"
                                if val < -5: return f"🔴 {val:+.2f}%"
                                return f"⚪ {val:+.2f}%"
                            
                            ranking_display = ranking.copy()
                            ranking_display['Eficiência (%)'] = ranking_display['Eficiência (%)'].apply(formatar_eficiencia)
                            st.dataframe(ranking_display[['Equipamento', 'Eficiência (%)']])
                            
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
                
                st.subheader("Histórico de Manutenções Realizadas")
                historico_manut_display = df_manutencoes[df_manutencoes['Cod_Equip'] == cod_sel].sort_values("Data", ascending=False)
                if not historico_manut_display.empty:
                    st.dataframe(historico_manut_display[['Data', 'Tipo_Servico', 'Hod_Hor_No_Servico']])
                else:
                    st.info("Nenhum registo de manutenção para este equipamento.")
        
                st.subheader("Histórico de Abastecimentos")
                historico_abast_display = consumo_eq.sort_values("Data", ascending=False)
                if not historico_abast_display.empty:
                    colunas_abast = ["Data", "Qtde_Litros", "Hod_Hor_Atual"]
                    st.dataframe(historico_abast_display[[c for c in colunas_abast if c in historico_abast_display]])
                else:
                    st.info("Nenhum registo de abastecimento para este equipamento.")
                            
        with tab_manut:
            st.header("🛠️ Controle Inteligente de Manutenção")
            
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

            with st.form("form_manutencao", clear_on_submit=True):
                    st.subheader("📝 Registrar Manutenção Realizada")
                    equip_label = st.selectbox(
                        "Selecione o Equipamento", 
                        options=df_frotas.sort_values("label")["label"], 
                        key="manut_equip"
                    )
                    
                    # --- INÍCIO DA CORREÇÃO ---
                    servicos_disponiveis = []
                    classe_selecionada = ""
                    if equip_label:
                        # Encontra a classe operacional do equipamento selecionado
                        classe_selecionada = df_frotas.loc[df_frotas['label'] == equip_label, 'Classe Operacional'].iloc[0]
                        # Busca os serviços configurados para ESSA classe na sessão
                        if classe_selecionada in st.session_state.intervalos_por_classe:
                            servicos_disponiveis = list(st.session_state.intervalos_por_classe[classe_selecionada].keys())
                    # --- FIM DA CORREÇÃO ---
        
                    tipo_servico = st.selectbox("Tipo de Serviço Realizado", options=servicos_disponiveis)
                    data_manutencao = st.date_input("Data da Manutenção")
                    hod_hor_servico = st.number_input("Leitura do Hodômetro/Horímetro no Serviço", min_value=0.01, format="%.2f")
        
                    submitted_manut = st.form_submit_button("Salvar Manutenção")
        
                    if submitted_manut:
                        if tipo_servico:
                            cod_equip = int(equip_label.split(" - ")[0])
                            dados_manut = {'cod_equip': cod_equip, 'data': data_manutencao.strftime("%Y-%m-%d"), 'tipo_servico': tipo_servico, 'hod_hor_servico': hod_hor_servico}
                            if inserir_manutencao(DB_PATH, dados_manut):
                                st.success("Manutenção registrada com sucesso!")
                                st.cache_data.clear()
                                st.rerun()
                        else:
                            st.warning("Não foi possível registrar. Verifique se esta classe de equipamento tem serviços configurados na aba 'Configurações'.")
                            
            st.markdown("---")
            st.subheader("📅 Previsão de Próximas Manutenções")
                
            df_previsao = prever_manutencoes(df_frotas, df, plan_df)

            if not df_previsao.empty:
                    # Filtra para mostrar apenas as previsões para os próximos 90 dias
                    st.dataframe(df_previsao[df_previsao['Dias Restantes'] <= 90])
            else:
                    st.info("Não há dados suficientes para gerar uma previsão de manutenções.")
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
                                        st.cache_data.clear()
                                        st.rerun()
        
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
                                            st.cache_data.clear()
                                            st.rerun()
                                            
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
                                                st.cache_data.clear()
                                                st.rerun()

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
                                                        st.cache_data.clear()
                                                        st.rerun()


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
                                            st.cache_data.clear()
                                            st.rerun()
            
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
                            nova_classe_op = st.text_input("Classe Operacional", value=dados_atuais['Classe Operacional'])
                            
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
                                    st.cache_data.clear()
                                    st.rerun()
                    

        with tab_config:
                st.header("⚙️ Configurar Intervalos e Metas por Classe")
                st.info("As alterações são salvas automaticamente para a sua sessão atual.")
                for classe, config in st.session_state.intervalos_por_classe.items():
                    with st.expander(f"**{classe}**"):
                        st.session_state.intervalos_por_classe[classe]['meta_consumo'] = st.number_input("Meta de Consumo (L/h ou Km/L)", value=float(config.get('meta_consumo', 0.0)), format="%.2f", step=0.1, key=f"{classe}_meta")
                        st.markdown("---")
                        servicos = config.get('servicos', {})
                        for servico_id, servico_info in servicos.items():
                            col1, col2 = st.columns(2)
                            novo_nome = col1.text_input("Nome do Serviço", value=servico_info.get('nome', servico_id), key=f"{classe}_{servico_id}_nome")
                            novo_intervalo = col2.number_input("Intervalo", value=servico_info.get('intervalo', 0), min_value=0, step=100, key=f"{classe}_{servico_id}_intervalo")
                            st.session_state.intervalos_por_classe[classe]['servicos'][servico_id]['nome'] = novo_nome
                            st.session_state.intervalos_por_classe[classe]['servicos'][servico_id]['intervalo'] = novo_intervalo
                        
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
                                    num_inseridos, mensagem = importar_abastecimentos_de_planilha(DB_PATH, arquivo_carregado)
                                
                                if num_inseridos > 0:
                                    st.success(f"**Sucesso!** {num_inseridos} registos foram importados. O dashboard será atualizado.")
                                    st.cache_data.clear()
                                    st.rerun()
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
                                        st.rerun()
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
                                            st.rerun()
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
                                            st.rerun()
                                        else:
                                            st.error(message)

                    
if __name__ == "__main__":
    main()
