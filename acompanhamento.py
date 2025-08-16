import streamlit as st
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime, date, timedelta
import os
import plotly.express as px
import hashlib
import json
import base64
import io

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
            df_comp_historico = pd.read_sql_query("SELECT rowid, * FROM componentes_historico", conn)
            df_checklist_regras = pd.read_sql_query("SELECT * FROM checklist_regras", conn)
            df_checklist_itens = pd.read_sql_query("SELECT * FROM checklist_itens", conn)
            df_checklist_historico = pd.read_sql_query("SELECT rowid, * FROM checklist_historico", conn)

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

        # Vincula informações de motorista aos abastecimentos (merge durante o load)
        try:
            with sqlite3.connect(db_path, check_same_thread=False) as conn:
                df_motoristas = pd.read_sql_query("SELECT codigo_pessoa, matricula, nome FROM motoristas", conn)
            if not df_motoristas.empty:
                df_merged = df_merged.merge(
                    df_motoristas.rename(columns={"codigo_pessoa": "Cod_Pessoa", "matricula": "Matricula", "nome": "Nome_Motorista"}),
                    on=["Cod_Pessoa", "Matricula"], how="left"
                )
        except Exception:
            pass
        
        # Garante que a classe operacional em df_frotas está atualizada
        classe_map = df_merged.dropna(subset=['Classe_Operacional']).groupby('Cod_Equip')['Classe_Operacional'].first()
        df_frotas['Classe_Operacional'] = df_frotas['Cod_Equip'].map(classe_map).fillna(df_frotas.get('Classe_Operacional'))

        # Adiciona coluna de tipo de combustível se não existir
        if 'tipo_combustivel' not in df_frotas.columns:
            df_frotas['tipo_combustivel'] = 'Diesel S500'  # Valor padrão
        
        # Garantir que a coluna existe e tem valores válidos
        if 'tipo_combustivel' in df_frotas.columns:
            # Preencher valores nulos com padrão
            df_frotas['tipo_combustivel'] = df_frotas['tipo_combustivel'].fillna('Diesel S500')
        else:
            df_frotas['tipo_combustivel'] = 'Diesel S500'

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
                Safra, "Mês", "Classe Operacional", Matricula, Cod_Pessoa
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        valores = (
            dados['cod_equip'],
            dados['data'],
            dados['qtde_litros'],
            dados['hod_hor_atual'],
            dados['safra'],
            dados['mes'],
            dados['classe_operacional'],
            dados.get('matricula'),
            dados.get('cod_pessoa')
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


def excluir_manutencao_componente(db_path: str, cod_equip: int, nome_componente: str, data: str, hod_hor: float) -> bool:
    """Exclui um registro de manutenção de componente do banco de dados usando uma combinação única de campos."""
    try:
        conn = sqlite3.connect(db_path, check_same_thread=False)
        cursor = conn.cursor()
        
        # Converter tipos de dados para garantir compatibilidade
        cod_equip = int(cod_equip)
        nome_componente = str(nome_componente)
        data = str(data)
        hod_hor = float(hod_hor)
        
        # Debug: verificar todos os registros na tabela
        cursor.execute("SELECT rowid, Cod_Equip, nome_componente, Data, Hod_Hor_No_Servico FROM componentes_historico")
        all_records = cursor.fetchall()
        
        # Debug: verificar se há registros com valores similares
        cursor.execute(
            "SELECT rowid, Cod_Equip, nome_componente, Data, Hod_Hor_No_Servico FROM componentes_historico WHERE Cod_Equip = ?", 
            (cod_equip,)
        )
        similar_records = cursor.fetchall()
        
        # Primeiro, vamos verificar se o registro existe
        cursor.execute(
            "SELECT COUNT(*) FROM componentes_historico WHERE Cod_Equip = ? AND nome_componente = ? AND Data = ? AND Hod_Hor_No_Servico = ?", 
            (cod_equip, nome_componente, data, hod_hor)
        )
        count = cursor.fetchone()[0]
        
        if count == 0:
            # Debug: retornar informações sobre o que foi encontrado
            debug_info = f"""
            Registro não encontrado para exclusão.
            
            Valores procurados (após conversão):
            - Cod_Equip: {cod_equip} (tipo: {type(cod_equip)})
            - Nome Componente: {nome_componente} (tipo: {type(nome_componente)})
            - Data: {data} (tipo: {type(data)})
            - Hod_Hor: {hod_hor} (tipo: {type(hod_hor)})
            
            Registros similares encontrados (mesmo Cod_Equip):
            {similar_records}
            
            Todos os registros na tabela:
            {all_records}
            """
            st.error(debug_info)
            return False
        
        # Agora vamos excluir
        cursor.execute(
            "DELETE FROM componentes_historico WHERE Cod_Equip = ? AND nome_componente = ? AND Data = ? AND Hod_Hor_No_Servico = ?", 
            (cod_equip, nome_componente, data, hod_hor)
        )
        
        # Forçar commit imediato
        conn.commit()
        
        # Verificar se foi realmente excluído
        rows_deleted = cursor.rowcount
        if rows_deleted > 0:
            # Verificar novamente se o registro foi realmente excluído
            cursor.execute(
                "SELECT COUNT(*) FROM componentes_historico WHERE Cod_Equip = ? AND nome_componente = ? AND Data = ? AND Hod_Hor_No_Servico = ?", 
                (cod_equip, nome_componente, data, hod_hor)
            )
            count_after = cursor.fetchone()[0]
            
            if count_after == 0:
                # Forçar sincronização do banco
                cursor.execute("PRAGMA wal_checkpoint(FULL)")
                cursor.execute("PRAGMA synchronous=FULL")
                conn.commit()
                
                # Salvar backup automático para persistência no Streamlit Cloud
                backup_success, backup_msg = save_backup_to_session_state()
                if backup_success:
                    st.success(f"Manutenção de componente excluída com sucesso! ({rows_deleted} registro(s) removido(s)) | Backup salvo: {backup_msg}")
                else:
                    st.success(f"Manutenção de componente excluída com sucesso! ({rows_deleted} registro(s) removido(s)) | Aviso: {backup_msg}")
                
                conn.close()
                return True
            else:
                st.error("Erro: Registro ainda existe após exclusão")
                conn.close()
                return False
        else:
            st.error("Nenhum registro foi excluído")
            conn.close()
            return False
            
    except Exception as e:
        st.error(f"Erro ao excluir manutenção de componente do banco de dados: {e}")
        return False
    finally:
        if 'conn' in locals():
            conn.close()


def excluir_manutencao(db_path: str, rowid: int) -> bool:
    """Exclui um registro de manutenção do banco de dados usando seu rowid."""
    try:
        conn = sqlite3.connect(db_path, check_same_thread=False)
        cursor = conn.cursor()
        sql = "DELETE FROM manutencoes WHERE rowid = ?"
        cursor.execute(sql, (rowid,))
        conn.commit()
        conn.close()
        return True
    except sqlite3.Error as e:
        st.error(f"Erro ao excluir manutenção do banco de dados: {e}")
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
                "Classe Operacional", ATIVO, tipo_combustivel
            ) VALUES (?, ?, ?, ?, ?, ?)
        """
        valores = (
            dados['cod_equip'],
            dados['descricao'],
            dados['placa'],
            dados['classe_op'],
            dados['ativo'],
            dados.get('tipo_combustivel', 'Diesel S500')
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
                "Cód. Equip." = ?, Data = ?, "Qtde Litros" = ?, Hod_Hor_Atual = ?, Safra = ?, Matricula = ?, Cod_Pessoa = ?
            WHERE rowid = ?
        """
        valores = (
            dados['cod_equip'], dados['data'], dados['qtde_litros'], dados['hod_hor_atual'], dados['safra'],
            dados.get('matricula'), dados.get('cod_pessoa'), rowid
        )
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


def editar_manutencao_componente(db_path: str, rowid: int, dados: dict) -> bool:
    """Edita um registro de manutenção de componente existente."""
    try:
        conn = sqlite3.connect(db_path, check_same_thread=False)
        cursor = conn.cursor()
        sql = """
            UPDATE componentes_historico 
            SET Cod_Equip = ?, nome_componente = ?, Observacoes = ?, Data = ?, Hod_Hor_No_Servico = ?
            WHERE rowid = ?
        """
        valores = (
            dados['cod_equip'],
            dados['componente'],
            dados['acao'],
            dados['data'],
            dados['hod_hor_servico'],
            rowid
        )
        cursor.execute(sql, valores)
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        st.error(f"Erro ao editar manutenção de componente no banco de dados: {e}")
        return False

def importar_abastecimentos_de_planilha(db_path: str, arquivo_carregado) -> tuple[int, int, str]:
    """Lê uma planilha, verifica por duplicados, e insere os novos dados. Aceita opcionalmente as colunas Matricula e Cod_Pessoa."""
    try:
        df_novo = pd.read_excel(arquivo_carregado)
        
        mapa_colunas = {
            "Cód. Equip.": "Cód. Equip.",
            "Data": "Data",
            "Qtde Litros": "Qtde Litros",
            "Hod. Hor. Atual": "Hod_Hor_Atual",
            "Safra": "Safra",
            "Mês": "Mês",
            "Classe Operacional": "Classe Operacional",
            "Matricula": "Matricula",
            "Cod_Pessoa": "Cod_Pessoa",
        }
        df_novo = df_novo.rename(columns={k: v for k, v in mapa_colunas.items() if k in df_novo.columns})

        colunas_necessarias = ["Cód. Equip.", "Data", "Qtde Litros", "Hod_Hor_Atual", "Safra", "Mês", "Classe Operacional"]
        colunas_opcionais = ["Matricula", "Cod_Pessoa"]
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

        colunas_insert = colunas_necessarias + [c for c in colunas_opcionais if c in df_para_inserir.columns]
        df_para_inserir_final = df_para_inserir[colunas_insert]
        registros = [tuple(x) for x in df_para_inserir_final.to_numpy()]
        
        cursor = conn.cursor()
        placeholders = ", ".join(["?"] * len(colunas_insert))
        sql = f"INSERT INTO abastecimentos ({', '.join(f'\"{col}\"' for col in colunas_insert)}) VALUES ({placeholders})"
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
                DESCRICAO_EQUIPAMENTO = ?, PLACA = ?, "Classe Operacional" = ?, ATIVO = ?, tipo_combustivel = ?
            WHERE COD_EQUIPAMENTO = ?
        """
        valores = (dados['descricao'], dados['placa'], dados['classe_op'], dados['ativo'], dados.get('tipo_combustivel', 'Diesel S500'), cod_equip)
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

def add_component_rule_advanced(classe, componente, intervalo, lubrificante_id=None, tipo_manutencao="Troca", capacidade_litros=0.0):
    """Adiciona uma nova regra de componente com informações de lubrificante e tipo de manutenção."""
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            
            # Verificar se a tabela tem as colunas necessárias
            cursor.execute("PRAGMA table_info(componentes_regras)")
            columns = [column[1] for column in cursor.fetchall()]
            
            # Adicionar colunas se não existirem
            if 'lubrificante_id' not in columns:
                cursor.execute("ALTER TABLE componentes_regras ADD COLUMN lubrificante_id INTEGER")
            if 'tipo_manutencao' not in columns:
                cursor.execute("ALTER TABLE componentes_regras ADD COLUMN tipo_manutencao TEXT DEFAULT 'Troca'")
            
            cursor.execute(
                "INSERT INTO componentes_regras (classe_operacional, nome_componente, intervalo_padrao, lubrificante_id, tipo_manutencao, capacidade_litros) VALUES (?, ?, ?, ?, ?, ?)",
                (classe, componente, intervalo, lubrificante_id, tipo_manutencao, capacidade_litros)
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

def add_component_service_advanced(cod_equip, componente, data, hod_hor, tipo_servico, lubrificante_utilizado=None, obs=""):
    """Adiciona um novo registo de serviço de componente com informações detalhadas."""
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            
            # Verificar se a tabela tem as colunas necessárias
            cursor.execute("PRAGMA table_info(componentes_historico)")
            columns = [column[1] for column in cursor.fetchall()]
            
            # Adicionar colunas se não existirem
            if 'tipo_servico' not in columns:
                cursor.execute("ALTER TABLE componentes_historico ADD COLUMN tipo_servico TEXT DEFAULT 'Troca'")
            if 'lubrificante_utilizado' not in columns:
                cursor.execute("ALTER TABLE componentes_historico ADD COLUMN lubrificante_utilizado TEXT")
            
            cursor.execute(
                "INSERT INTO componentes_historico (Cod_Equip, nome_componente, Data, Hod_Hor_No_Servico, tipo_servico, lubrificante_utilizado, Observacoes) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (cod_equip, componente, data, hod_hor, tipo_servico, lubrificante_utilizado, obs)
            )
            conn.commit()
        return True, "Serviço de componente registado com sucesso."
    except Exception as e:
        return False, f"Erro ao registar serviço: {e}"

def get_component_status(cod_equip, componente):
    """Obtém o status atual de um componente específico de um equipamento."""
    try:
        with sqlite3.connect(DB_PATH) as conn:
            # Buscar a última manutenção do componente
            query = """
            SELECT Data, Hod_Hor_No_Servico, tipo_servico, lubrificante_utilizado, Observacoes
            FROM componentes_historico 
            WHERE Cod_Equip = ? AND nome_componente = ?
            ORDER BY Data DESC, Hod_Hor_No_Servico DESC
            LIMIT 1
            """
            df_ultima = pd.read_sql_query(query, conn, params=(cod_equip, componente))
            
            # Buscar a regra do componente para obter o intervalo
            query_regra = """
            SELECT intervalo_padrao, lubrificante_id, tipo_manutencao
            FROM componentes_regras cr
            JOIN frotas f ON cr.classe_operacional = f."Classe Operacional"
            WHERE f.COD_EQUIPAMENTO = ? AND cr.nome_componente = ?
            """
            df_regra = pd.read_sql_query(query_regra, conn, params=(cod_equip, componente))
            
            # Buscar o hodômetro/horímetro atual do equipamento
            query_hod = """
            SELECT Hod_Hor_Atual FROM abastecimentos 
            WHERE Cod_Equip = ? 
            ORDER BY Data DESC, Hod_Hor_Atual DESC 
            LIMIT 1
            """
            df_hod = pd.read_sql_query(query_hod, conn, params=(cod_equip,))
            
            return df_ultima, df_regra, df_hod
            
    except Exception as e:
        st.error(f"Erro ao obter status do componente: {e}")
        return None, None, None

def get_component_maintenance_count(cod_equip, componente):
    """Obtém o número total de manutenções realizadas em um componente."""
    try:
        with sqlite3.connect(DB_PATH) as conn:
            query = """
            SELECT COUNT(*) as total_manutencoes,
                   COUNT(CASE WHEN tipo_servico = 'Troca' THEN 1 END) as total_trocas,
                   COUNT(CASE WHEN tipo_servico = 'Remonta' THEN 1 END) as total_remontas
            FROM componentes_historico 
            WHERE Cod_Equip = ? AND nome_componente = ?
            """
            df_count = pd.read_sql_query(query, conn, params=(cod_equip, componente))
            return df_count.iloc[0] if not df_count.empty else {'total_manutencoes': 0, 'total_trocas': 0, 'total_remontas': 0}
            
    except Exception as e:
        st.error(f"Erro ao obter contagem de manutenções: {e}")
        return {'total_manutencoes': 0, 'total_trocas': 0, 'total_remontas': 0}

def editar_manutencao_componente_advanced(DB_PATH, rowid, dados_editados):
    """Edita uma manutenção de componente com informações avançadas."""
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            
            # Verificar se a tabela tem as colunas necessárias
            cursor.execute("PRAGMA table_info(componentes_historico)")
            columns = [column[1] for column in cursor.fetchall()]
            
            # Adicionar colunas se não existirem
            if 'tipo_servico' not in columns:
                cursor.execute("ALTER TABLE componentes_historico ADD COLUMN tipo_servico TEXT DEFAULT 'Troca'")
            if 'lubrificante_utilizado' not in columns:
                cursor.execute("ALTER TABLE componentes_historico ADD COLUMN lubrificante_utilizado TEXT")
            
            # Atualizar os dados
            cursor.execute("""
                UPDATE componentes_historico 
                SET Cod_Equip = ?, nome_componente = ?, Data = ?, Hod_Hor_No_Servico = ?, 
                    Observacoes = ?, tipo_servico = ?, lubrificante_utilizado = ?
                WHERE rowid = ?
            """, (
                dados_editados['cod_equip'],
                dados_editados['componente'],
                dados_editados['data'],
                dados_editados['hod_hor_servico'],
                dados_editados['acao'],
                dados_editados['tipo_servico'],
                dados_editados['lubrificante_utilizado'],
                rowid
            ))
            conn.commit()
        return True, "Manutenção de componente atualizada com sucesso."
    except Exception as e:
        return False, f"Erro ao atualizar manutenção de componente: {e}"

def update_component_rule(rule_id, nome_componente, intervalo, lubrificante_id=None, tipo_manutencao="Troca"):
    """Atualiza uma regra de componente existente."""
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            
            # Verificar se a tabela tem as colunas necessárias
            cursor.execute("PRAGMA table_info(componentes_regras)")
            columns = [column[1] for column in cursor.fetchall()]
            
            # Adicionar colunas se não existirem
            if 'lubrificante_id' not in columns:
                cursor.execute("ALTER TABLE componentes_regras ADD COLUMN lubrificante_id INTEGER")
            if 'tipo_manutencao' not in columns:
                cursor.execute("ALTER TABLE componentes_regras ADD COLUMN tipo_manutencao TEXT DEFAULT 'Troca'")
            
            # Atualizar os dados
            cursor.execute("""
                UPDATE componentes_regras 
                SET nome_componente = ?, intervalo_padrao = ?, lubrificante_id = ?, tipo_manutencao = ?
                WHERE id_regra = ?
            """, (nome_componente, intervalo, lubrificante_id, tipo_manutencao, rule_id))
            conn.commit()
        return True, f"Componente '{nome_componente}' atualizado com sucesso."
    except Exception as e:
        return False, f"Erro ao atualizar componente: {e}"


def get_frota_combustivel(cod_equip):
    """Obtém o tipo de combustível de uma frota específica."""
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT tipo_combustivel FROM frotas WHERE COD_EQUIPAMENTO = ?", (cod_equip,))
            result = cursor.fetchone()
            return result[0] if result else None
    except Exception as e:
        st.error(f"Erro ao obter tipo de combustível: {e}")
        return None


def update_frota_combustivel(cod_equip, tipo_combustivel):
    """Atualiza o tipo de combustível de uma frota específica."""
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE frotas SET tipo_combustivel = ? WHERE COD_EQUIPAMENTO = ?", (tipo_combustivel, cod_equip))
            conn.commit()
        return True, f"Tipo de combustível atualizado para {tipo_combustivel}"
    except Exception as e:
        return False, f"Erro ao atualizar tipo de combustível: {e}"


def update_classe_combustivel(classe_operacional, tipo_combustivel):
    """Atualiza o tipo de combustível de todas as frotas de uma classe."""
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE frotas SET tipo_combustivel = ? WHERE \"Classe Operacional\" = ?", (tipo_combustivel, classe_operacional))
            rows_updated = cursor.rowcount
            conn.commit()
        return True, f"Tipo de combustível atualizado para {tipo_combustivel} em {rows_updated} frotas da classe {classe_operacional}"
    except Exception as e:
        return False, f"Erro ao atualizar tipo de combustível da classe: {e}"


def add_tipo_combustivel_column():
    """Adiciona a coluna tipo_combustivel à tabela frotas se ela não existir."""
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            # Verificar se a coluna existe
            cursor.execute("PRAGMA table_info(frotas)")
            columns = [column[1] for column in cursor.fetchall()]
            
            if 'tipo_combustivel' not in columns:
                cursor.execute("ALTER TABLE frotas ADD COLUMN tipo_combustivel TEXT DEFAULT 'Diesel S500'")
                conn.commit()
                return True, "Coluna tipo_combustivel adicionada com sucesso"
            else:
                return True, "Coluna tipo_combustivel já existe"
    except Exception as e:
        return False, f"Erro ao adicionar coluna tipo_combustivel: {e}"


def ensure_motoristas_schema():
    """Garante a existência da tabela de motoristas e das colunas de vínculo em abastecimentos."""
    try:
        with sqlite3.connect(DB_PATH, check_same_thread=False) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS motoristas (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    codigo_pessoa TEXT,
                    matricula TEXT UNIQUE,
                    nome TEXT,
                    ativo TEXT DEFAULT 'ATIVO'
                )
                """
            )
            cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_motoristas_matricula ON motoristas(matricula)")
            cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_motoristas_codigo_pessoa ON motoristas(codigo_pessoa)")

            cursor.execute("PRAGMA table_info(abastecimentos)")
            cols = [c[1] for c in cursor.fetchall()]
            if 'Matricula' not in cols:
                cursor.execute("ALTER TABLE abastecimentos ADD COLUMN Matricula TEXT")
            if 'Cod_Pessoa' not in cols:
                cursor.execute("ALTER TABLE abastecimentos ADD COLUMN Cod_Pessoa TEXT")
            conn.commit()
        return True, "Esquema de motoristas verificado"
    except Exception as e:
        return False, f"Erro ao verificar esquema de motoristas: {e}"


def get_all_motoristas() -> pd.DataFrame:
    """Retorna o DataFrame de motoristas."""
    try:
        with sqlite3.connect(DB_PATH, check_same_thread=False) as conn:
            return pd.read_sql_query("SELECT * FROM motoristas", conn)
    except Exception:
        return pd.DataFrame(columns=['id', 'codigo_pessoa', 'matricula', 'nome', 'ativo'])


def importar_motoristas_de_planilha(db_path: str, arquivo_carregado):
    """Importa motoristas a partir de planilha Excel. Espera colunas: Matricula, Nome e opcional Cod_Pessoa/Código Pessoa."""
    try:
        df_mot = pd.read_excel(arquivo_carregado)
        df_mot.columns = [c.strip() for c in df_mot.columns]
        renomeios = {
            'Matrícula': 'Matricula', 'matricula': 'Matricula', 'MATRICULA': 'Matricula',
            'Nome': 'Nome', 'nome': 'Nome', 'NOME': 'Nome',
            'Cod_Pessoa': 'Cod_Pessoa', 'Código Pessoa': 'Cod_Pessoa', 'codigo_pessoa': 'Cod_Pessoa', 'CODIGO_PESSOA': 'Cod_Pessoa'
        }
        df_mot.rename(columns={k: v for k, v in renomeios.items() if k in df_mot.columns}, inplace=True)
        obrig = ['Matricula', 'Nome']
        faltando = [c for c in obrig if c not in df_mot.columns]
        if faltando:
            return 0, 0, f"Erro: Colunas obrigatórias não encontradas: {', '.join(faltando)}"
        if 'Cod_Pessoa' not in df_mot.columns:
            df_mot['Cod_Pessoa'] = None
        df_mot = df_mot.dropna(subset=['Matricula', 'Nome']).copy()
        df_mot['Matricula'] = df_mot['Matricula'].astype(str).str.strip()
        df_mot['Nome'] = df_mot['Nome'].astype(str).str.strip()
        df_mot['Cod_Pessoa'] = df_mot['Cod_Pessoa'].astype(str).str.strip()
        df_mot = df_mot.drop_duplicates(subset=['Matricula'])
        with sqlite3.connect(db_path, check_same_thread=False) as conn:
            existentes = pd.read_sql_query("SELECT matricula FROM motoristas", conn)
            set_exist = set(existentes['matricula'].astype(str)) if not existentes.empty else set()
            df_novos = df_mot[~df_mot['Matricula'].isin(set_exist)].copy()
            if df_novos.empty:
                return 0, len(df_mot), "Nenhum motorista novo para importar. Todos já existem."
            registros = [
                (row.get('Cod_Pessoa', None), row['Matricula'], row['Nome'], 'ATIVO')
                for _, row in df_novos.iterrows()
            ]
            cur = conn.cursor()
            cur.executemany(
                "INSERT INTO motoristas (codigo_pessoa, matricula, nome, ativo) VALUES (?, ?, ?, ?)",
                registros
            )
            conn.commit()
            inseridos = cur.rowcount if cur.rowcount is not None else len(registros)
            duplicados = len(df_mot) - len(df_novos)
            return inseridos, duplicados, f"{inseridos} motoristas importados com sucesso. {duplicados} já existiam."
    except Exception as e:
        return 0, 0, f"Ocorreu um erro inesperado durante a importação de motoristas: {e}"
    
def ensure_pneus_schema():
    """Garante a existência da tabela de histórico de pneus."""
    try:
        with sqlite3.connect(DB_PATH, check_same_thread=False) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS pneus_historico (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    Cod_Equip INTEGER,
                    posicao TEXT,
                    marca TEXT,
                    modelo TEXT,
                    data_instalacao TEXT,
                    hodometro_instalacao REAL,
                    vida_util_km REAL,
                    observacoes TEXT,
                    status TEXT DEFAULT 'Ativo',
                    vida_atual INTEGER DEFAULT 1
                )
            """)
            # Adiciona colunas se não existirem
            cursor.execute("PRAGMA table_info(pneus_historico)")
            cols = [c[1] for c in cursor.fetchall()]
            if 'status' not in cols:
                cursor.execute("ALTER TABLE pneus_historico ADD COLUMN status TEXT DEFAULT 'Ativo'")
            if 'vida_atual' not in cols:
                cursor.execute("ALTER TABLE pneus_historico ADD COLUMN vida_atual INTEGER DEFAULT 1")
            conn.commit()
        return True, "Tabela de pneus verificada"
    except Exception as e:
        return False, f"Erro ao criar tabela de pneus: {e}"

def importar_pneus_de_planilha(db_path: str, arquivo_carregado):
    """Importa histórico de pneus de uma planilha Excel, verificando duplicatas."""
    try:
        df_pneus = pd.read_excel(arquivo_carregado)
        df_pneus.columns = [c.strip() for c in df_pneus.columns]
        obrig = ['Cod_Equip', 'posicao', 'marca', 'modelo', 'data_instalacao', 'hodometro_instalacao', 'vida_util_km']
        faltando = [c for c in obrig if c not in df_pneus.columns]
        if faltando:
            return 0, 0, f"Colunas obrigatórias faltando: {', '.join(faltando)}"
        
        if 'observacoes' not in df_pneus.columns:
            df_pneus['observacoes'] = ""
        
        # Limpar dados e remover linhas com valores nulos obrigatórios
        df_pneus = df_pneus.dropna(subset=['Cod_Equip', 'posicao'])
        
        # Normalizar tipos de dados
        df_pneus['Cod_Equip'] = df_pneus['Cod_Equip'].astype(str)
        df_pneus['posicao'] = df_pneus['posicao'].astype(str).str.strip()
        df_pneus['data_instalacao'] = pd.to_datetime(df_pneus['data_instalacao']).dt.strftime('%Y-%m-%d')
        
        # Remover duplicatas na própria planilha baseada em chave única
        df_pneus = df_pneus.drop_duplicates(subset=['Cod_Equip', 'posicao', 'data_instalacao', 'hodometro_instalacao'])
        
        with sqlite3.connect(db_path, check_same_thread=False) as conn:
            # Buscar registros existentes para verificar duplicatas
            df_existente = pd.read_sql_query("SELECT Cod_Equip, posicao, data_instalacao, hodometro_instalacao FROM pneus_historico", conn)
            
            if not df_existente.empty:
                # Normalizar dados existentes para comparação
                df_existente['Cod_Equip'] = df_existente['Cod_Equip'].astype(str)
                df_existente['posicao'] = df_existente['posicao'].astype(str).str.strip()
                df_existente['data_instalacao'] = pd.to_datetime(df_existente['data_instalacao']).dt.strftime('%Y-%m-%d')
                
                # Criar chaves únicas para comparação
                df_pneus['chave_unica'] = (df_pneus['Cod_Equip'] + '_' + 
                                          df_pneus['posicao'] + '_' + 
                                          df_pneus['data_instalacao'] + '_' + 
                                          df_pneus['hodometro_instalacao'].astype(str))
                
                df_existente['chave_unica'] = (df_existente['Cod_Equip'] + '_' + 
                                              df_existente['posicao'] + '_' + 
                                              df_existente['data_instalacao'] + '_' + 
                                              df_existente['hodometro_instalacao'].astype(str))
                
                # Filtrar apenas registros que não existem
                df_para_inserir = df_pneus[~df_pneus['chave_unica'].isin(df_existente['chave_unica'])]
            else:
                df_para_inserir = df_pneus
            
            num_duplicados = len(df_pneus) - len(df_para_inserir)
            
            if df_para_inserir.empty:
                return 0, num_duplicados, "Nenhum pneu novo para importar. Todos os registros da planilha já existem na base de dados."
            
            # Preparar registros para inserção
            colunas_insert = obrig + ['observacoes']
            df_para_inserir_final = df_para_inserir[colunas_insert]
            registros = [tuple(x) for x in df_para_inserir_final.fillna('').to_numpy()]
            
            cur = conn.cursor()
            placeholders = ", ".join(["?"] * len(colunas_insert))
            sql = f"INSERT INTO pneus_historico ({', '.join(f'\"{col}\"' for col in colunas_insert)}) VALUES ({placeholders})"
            cur.executemany(sql, registros)
            conn.commit()
            
            num_inseridos = len(registros)
            
            mensagem_sucesso = f"{num_inseridos} pneus novos foram importados com sucesso."
            if num_duplicados > 0:
                mensagem_sucesso += f" {num_duplicados} registros duplicados foram ignorados."
            
            return num_inseridos, num_duplicados, mensagem_sucesso
            
    except Exception as e:
        return 0, 0, f"Erro ao importar pneus: {e}"

def get_pneus_historico(cod_equip=None):
    """Retorna o histórico de pneus, opcionalmente filtrando por frota."""
    try:
        with sqlite3.connect(DB_PATH, check_same_thread=False) as conn:
            query = "SELECT * FROM pneus_historico"
            params = ()
            if cod_equip:
                query += " WHERE Cod_Equip = ?"
                params = (cod_equip,)
            return pd.read_sql_query(query, conn, params=params)
    except Exception:
        return pd.DataFrame()

def ensure_precos_combustivel_schema():
    """Garante a existência da tabela de preços por tipo de combustível."""
    try:
        with sqlite3.connect(DB_PATH, check_same_thread=False) as conn:
            cur = conn.cursor()
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS precos_combustivel (
                    tipo_combustivel TEXT PRIMARY KEY,
                    preco REAL
                )
                """
            )
            tipos = ['Diesel S500', 'Diesel S10', 'Gasolina', 'Etanol', 'Biodiesel']
            for t in tipos:
                cur.execute("INSERT OR IGNORE INTO precos_combustivel (tipo_combustivel, preco) VALUES (?, ?)", (t, NULL))
            conn.commit()
        return True, "Tabela de preços verificada"
    except Exception as e:
        return False, f"Erro ao verificar tabela de preços: {e}"


def get_precos_combustivel_map() -> dict:
    """Retorna um dicionário {tipo_combustivel: preco}."""
    try:
        with sqlite3.connect(DB_PATH, check_same_thread=False) as conn:
            dfp = pd.read_sql_query("SELECT tipo_combustivel, preco FROM precos_combustivel", conn)
        return {row['tipo_combustivel']: row['preco'] for _, row in dfp.iterrows()}
    except Exception:
        return {}


def upsert_preco_combustivel(tipo: str, preco: float) -> tuple[bool, str]:
    """Cria/atualiza preço para um tipo de combustível."""
    try:
        with sqlite3.connect(DB_PATH, check_same_thread=False) as conn:
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO precos_combustivel (tipo_combustivel, preco) VALUES (?, ?) ON CONFLICT(tipo_combustivel) DO UPDATE SET preco=excluded.preco",
                (tipo, preco)
            )
            conn.commit()
        return True, f"Preço atualizado para {tipo}"
    except Exception as e:
        return False, f"Erro ao atualizar preço: {e}"
    
def ensure_lubrificantes_schema():
    """Garante a existência da tabela de lubrificantes, movimentações e almoxarifados."""
    try:
        with sqlite3.connect(DB_PATH, check_same_thread=False) as conn:
            cursor = conn.cursor()
            
            # Tabela de lubrificantes
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS lubrificantes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    nome TEXT,
                    viscosidade TEXT,
                    quantidade_estoque REAL,
                    unidade TEXT,
                    observacoes TEXT
                )
            """)
            
            # Verificar e adicionar coluna 'tipo' se não existir
            cursor.execute("PRAGMA table_info(lubrificantes)")
            cols = [c[1] for c in cursor.fetchall()]
            if 'tipo' not in cols:
                cursor.execute("ALTER TABLE lubrificantes ADD COLUMN tipo TEXT DEFAULT 'óleo'")
            
            # Tabela de almoxarifados
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS almoxarifados (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    nome TEXT NOT NULL,
                    tipo TEXT DEFAULT 'fixo', -- 'fixo' para oficina, 'movel' para caminhões
                    localizacao TEXT,
                    responsavel TEXT,
                    observacoes TEXT,
                    ativo BOOLEAN DEFAULT 1
                )
            """)
            
            # Tabela de estoque por almoxarifado
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS almoxarifado_estoque (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    id_almoxarifado INTEGER,
                    id_lubrificante INTEGER,
                    quantidade_estoque REAL DEFAULT 0,
                    unidade TEXT,
                    data_atualizacao TEXT,
                    FOREIGN KEY(id_almoxarifado) REFERENCES almoxarifados(id),
                    FOREIGN KEY(id_lubrificante) REFERENCES lubrificantes(id),
                    UNIQUE(id_almoxarifado, id_lubrificante)
                )
            """)
            
            # Tabela de movimentações (atualizada para incluir almoxarifado)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS lubrificantes_movimentacoes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    id_lubrificante INTEGER,
                    id_almoxarifado INTEGER,
                    tipo TEXT, -- 'entrada' ou 'saida'
                    quantidade REAL,
                    data TEXT,
                    cod_equip INTEGER,
                    observacoes TEXT,
                    FOREIGN KEY(id_lubrificante) REFERENCES lubrificantes(id),
                    FOREIGN KEY(id_almoxarifado) REFERENCES almoxarifados(id)
                )
            """)
            
            # Verificar se a coluna id_almoxarifado existe na tabela de movimentações
            cursor.execute("PRAGMA table_info(lubrificantes_movimentacoes)")
            cols_mov = [c[1] for c in cursor.fetchall()]
            if 'id_almoxarifado' not in cols_mov:
                cursor.execute("ALTER TABLE lubrificantes_movimentacoes ADD COLUMN id_almoxarifado INTEGER")
            
            conn.commit()
        return True, "Tabelas de lubrificantes e almoxarifados verificadas"
    except Exception as e:
        return False, f"Erro ao criar tabelas de lubrificantes: {e}"
    
def add_almoxarifado(nome, tipo="fixo", localizacao="", responsavel="", observacoes=""):
    """Adiciona um novo almoxarifado."""
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO almoxarifados (nome, tipo, localizacao, responsavel, observacoes) VALUES (?, ?, ?, ?, ?)",
                (nome, tipo, localizacao, responsavel, observacoes)
            )
            conn.commit()
        return True, "Almoxarifado cadastrado com sucesso!"
    except Exception as e:
        return False, f"Erro: {e}"

def get_almoxarifados():
    """Retorna todos os almoxarifados ativos."""
    try:
        with sqlite3.connect(DB_PATH) as conn:
            df = pd.read_sql("SELECT * FROM almoxarifados WHERE ativo = 1 ORDER BY nome", conn)
        return df
    except Exception as e:
        return pd.DataFrame()

def get_estoque_por_almoxarifado(id_lubrificante):
    """Retorna o estoque de um lubrificante distribuído por almoxarifados."""
    try:
        with sqlite3.connect(DB_PATH) as conn:
            query = """
            SELECT 
                a.nome as almoxarifado,
                a.tipo,
                COALESCE(ae.quantidade_estoque, 0) as quantidade,
                COALESCE(ae.unidade, l.unidade) as unidade,
                a.localizacao,
                a.responsavel
            FROM almoxarifados a
            CROSS JOIN lubrificantes l
            LEFT JOIN almoxarifado_estoque ae ON a.id = ae.id_almoxarifado AND l.id = ae.id_lubrificante
            WHERE l.id = ? AND a.ativo = 1
            ORDER BY a.nome
            """
            df = pd.read_sql(query, conn, params=(id_lubrificante,))
        return df
    except Exception as e:
        return pd.DataFrame()

def atualizar_estoque_almoxarifado(id_almoxarifado, id_lubrificante, quantidade, unidade):
    """Atualiza o estoque de um lubrificante em um almoxarifado específico."""
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT OR REPLACE INTO almoxarifado_estoque 
                (id_almoxarifado, id_lubrificante, quantidade_estoque, unidade, data_atualizacao) 
                VALUES (?, ?, ?, ?, ?)
            """, (id_almoxarifado, id_lubrificante, quantidade, unidade, date.today().strftime("%Y-%m-%d")))
            conn.commit()
        return True, "Estoque atualizado com sucesso!"
    except Exception as e:
        return False, f"Erro ao atualizar estoque: {e}"

def add_lubrificante(nome, viscosidade, quantidade, unidade, observacoes=""):
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO lubrificantes (nome, viscosidade, quantidade_estoque, unidade, observacoes) VALUES (?, ?, ?, ?, ?)",
                (nome, viscosidade, quantidade, unidade, observacoes)
            )
            conn.commit()
        return True, "Lubrificante cadastrado!"
    except Exception as e:
        return False, f"Erro: {e}"

def importar_lubrificantes_de_planilha(db_path: str, arquivo_carregado):
    """Importa lubrificantes de uma planilha Excel, verificando duplicatas."""
    try:
        df_lub = pd.read_excel(arquivo_carregado)
        df_lub.columns = [c.strip() for c in df_lub.columns]
        
        # Mapeamento de colunas
        mapa_colunas = {
            'nome': 'nome',
            'tipo': 'tipo',
            'viscosidade': 'viscosidade',
            'quantidade_estoque': 'quantidade_estoque',
            'unidade': 'unidade',
            'observacoes': 'observacoes'
        }
        
        # Normalizar nomes de colunas
        for col_orig, col_norm in mapa_colunas.items():
            if col_orig in df_lub.columns:
                df_lub = df_lub.rename(columns={col_orig: col_norm})
        
        # Verificar colunas obrigatórias
        obrig = ['nome']
        faltando = [c for c in obrig if c not in df_lub.columns]
        if faltando:
            return 0, 0, f"Colunas obrigatórias faltando: {', '.join(faltando)}"
        
        # Adicionar colunas opcionais se não existirem
        if 'tipo' not in df_lub.columns:
            df_lub['tipo'] = 'óleo'
        if 'viscosidade' not in df_lub.columns:
            df_lub['viscosidade'] = ''
        if 'quantidade_estoque' not in df_lub.columns:
            df_lub['quantidade_estoque'] = 0
        if 'unidade' not in df_lub.columns:
            df_lub['unidade'] = 'L'
        if 'observacoes' not in df_lub.columns:
            df_lub['observacoes'] = ''
        
        # Limpar e normalizar dados
        df_lub = df_lub.dropna(subset=['nome'])
        df_lub['nome'] = df_lub['nome'].astype(str).str.strip()
        df_lub['tipo'] = df_lub['tipo'].astype(str).str.strip().fillna('óleo')
        df_lub['viscosidade'] = df_lub['viscosidade'].astype(str).str.strip().fillna('')
        df_lub['quantidade_estoque'] = pd.to_numeric(df_lub['quantidade_estoque'], errors='coerce').fillna(0)
        df_lub['unidade'] = df_lub['unidade'].astype(str).str.strip().fillna('L')
        df_lub['observacoes'] = df_lub['observacoes'].astype(str).str.strip().fillna('')
        
        # Remover duplicatas na própria planilha baseada no nome
        df_lub = df_lub.drop_duplicates(subset=['nome'])
        
        with sqlite3.connect(db_path, check_same_thread=False) as conn:
            # Garantir que a tabela existe com a coluna tipo
            ensure_lubrificantes_schema()
            
            # Buscar lubrificantes existentes
            df_existente = pd.read_sql_query("SELECT nome FROM lubrificantes", conn)
            
            if not df_existente.empty:
                # Normalizar nomes existentes para comparação
                df_existente['nome'] = df_existente['nome'].astype(str).str.strip()
                
                # Filtrar apenas registros que não existem
                df_para_inserir = df_lub[~df_lub['nome'].isin(df_existente['nome'])]
            else:
                df_para_inserir = df_lub
            
            num_duplicados = len(df_lub) - len(df_para_inserir)
            
            if df_para_inserir.empty:
                return 0, num_duplicados, "Nenhum lubrificante novo para importar. Todos os registros da planilha já existem na base de dados."
            
            # Preparar registros para inserção
            colunas_insert = ['nome', 'tipo', 'viscosidade', 'quantidade_estoque', 'unidade', 'observacoes']
            df_para_inserir_final = df_para_inserir[colunas_insert]
            registros = [tuple(x) for x in df_para_inserir_final.to_numpy()]
            
            cur = conn.cursor()
            placeholders = ", ".join(["?"] * len(colunas_insert))
            sql = f"INSERT INTO lubrificantes ({', '.join(f'\"{col}\"' for col in colunas_insert)}) VALUES ({placeholders})"
            cur.executemany(sql, registros)
            conn.commit()
            
            num_inseridos = len(registros)
            
            mensagem_sucesso = f"{num_inseridos} lubrificantes novos foram importados com sucesso."
            if num_duplicados > 0:
                mensagem_sucesso += f" {num_duplicados} registros duplicados foram ignorados."
            
            return num_inseridos, num_duplicados, mensagem_sucesso
            
    except Exception as e:
        return 0, 0, f"Erro ao importar lubrificantes: {e}"

def importar_componentes_de_planilha(db_path: str, arquivo_carregado, classe_operacional: str):
    """Importa componentes de uma planilha Excel, verificando duplicatas e criando lubrificantes se necessário."""
    try:
        df_comp = pd.read_excel(arquivo_carregado)
        df_comp.columns = [c.strip() for c in df_comp.columns]
        
        # Mapeamento de colunas
        mapa_colunas = {
            'nome_componente': 'nome_componente',
            'componente': 'nome_componente',
            'intervalo_padrao': 'intervalo_padrao',
            'intervalo': 'intervalo_padrao',
            'lubrificante_nome': 'lubrificante_nome',
            'lubrificante': 'lubrificante_nome',
            'capacidade_litros': 'capacidade_litros',
            'capacidade': 'capacidade_litros'
        }
        
        # Normalizar nomes de colunas
        for col_orig, col_norm in mapa_colunas.items():
            if col_orig in df_comp.columns:
                df_comp = df_comp.rename(columns={col_orig: col_norm})
        
        # Verificar colunas obrigatórias
        obrig = ['nome_componente', 'intervalo_padrao']
        faltando = [c for c in obrig if c not in df_comp.columns]
        if faltando:
            return 0, 0, 0, f"Colunas obrigatórias faltando: {', '.join(faltando)}"
        
        # Adicionar colunas opcionais se não existirem
        if 'lubrificante_nome' not in df_comp.columns:
            df_comp['lubrificante_nome'] = None
        if 'capacidade_litros' not in df_comp.columns:
            df_comp['capacidade_litros'] = 0.0
        
        # Limpar e normalizar dados
        df_comp = df_comp.dropna(subset=['nome_componente'])
        df_comp['nome_componente'] = df_comp['nome_componente'].astype(str).str.strip()
        df_comp['intervalo_padrao'] = pd.to_numeric(df_comp['intervalo_padrao'], errors='coerce')
        df_comp = df_comp.dropna(subset=['intervalo_padrao'])
        df_comp['lubrificante_nome'] = df_comp['lubrificante_nome'].astype(str).str.strip().replace('nan', None)
        df_comp['capacidade_litros'] = pd.to_numeric(df_comp['capacidade_litros'], errors='coerce').fillna(0.0)
        
        # Remover duplicatas na própria planilha baseada no nome do componente
        df_comp = df_comp.drop_duplicates(subset=['nome_componente'])
        
        with sqlite3.connect(db_path, check_same_thread=False) as conn:
            # Garantir que as tabelas existem
            ensure_lubrificantes_schema()
            
            # Verificar se a tabela componentes_regras tem a coluna capacidade_litros
            cursor = conn.cursor()
            cursor.execute("PRAGMA table_info(componentes_regras)")
            columns = [column[1] for column in cursor.fetchall()]
            
            if 'capacidade_litros' not in columns:
                cursor.execute("ALTER TABLE componentes_regras ADD COLUMN capacidade_litros REAL DEFAULT 0.0")
            
            # Buscar componentes existentes na classe
            df_existente = pd.read_sql_query(
                "SELECT nome_componente FROM componentes_regras WHERE classe_operacional = ?", 
                conn, params=(classe_operacional,)
            )
            
            if not df_existente.empty:
                # Normalizar nomes existentes para comparação
                df_existente['nome_componente'] = df_existente['nome_componente'].astype(str).str.strip()
                
                # Filtrar apenas registros que não existem na classe
                df_para_inserir = df_comp[~df_comp['nome_componente'].isin(df_existente['nome_componente'])]
            else:
                df_para_inserir = df_comp
            
            num_duplicados = len(df_comp) - len(df_para_inserir)
            
            if df_para_inserir.empty:
                return 0, num_duplicados, 0, "Nenhum componente novo para importar. Todos os registros da planilha já existem na classe selecionada."
            
            # Processar lubrificantes
            lubrificantes_criados = 0
            for _, row in df_para_inserir.iterrows():
                if pd.notna(row['lubrificante_nome']) and row['lubrificante_nome']:
                    # Verificar se o lubrificante existe
                    df_lub_existente = pd.read_sql_query(
                        "SELECT id FROM lubrificantes WHERE nome = ?", 
                        conn, params=(row['lubrificante_nome'],)
                    )
                    
                    if df_lub_existente.empty:
                        # Criar lubrificante automaticamente
                        cur = conn.cursor()
                        cur.execute(
                            "INSERT INTO lubrificantes (nome, tipo, viscosidade, quantidade_estoque, unidade, observacoes) VALUES (?, ?, ?, ?, ?, ?)",
                            (row['lubrificante_nome'], 'óleo', '', 0, 'L', f'Criado automaticamente durante importação de componentes')
                        )
                        lubrificantes_criados += 1
                        
                        # Buscar o ID do lubrificante criado
                        lub_id = cur.lastrowid
                    else:
                        lub_id = df_lub_existente.iloc[0]['id']
                else:
                    lub_id = None
                
                # Inserir componente
                cur = conn.cursor()
                cur.execute(
                    "INSERT INTO componentes_regras (classe_operacional, nome_componente, intervalo_padrao, lubrificante_id, capacidade_litros) VALUES (?, ?, ?, ?, ?)",
                    (classe_operacional, row['nome_componente'], row['intervalo_padrao'], lub_id, row['capacidade_litros'])
                )
            
            conn.commit()
            num_inseridos = len(df_para_inserir)
            
            mensagem_sucesso = f"{num_inseridos} componentes foram importados com sucesso para a classe '{classe_operacional}'."
            if num_duplicados > 0:
                mensagem_sucesso += f" {num_duplicados} componentes duplicados foram ignorados."
            if lubrificantes_criados > 0:
                mensagem_sucesso += f" {lubrificantes_criados} lubrificantes foram criados automaticamente."
            
            return num_inseridos, num_duplicados, lubrificantes_criados, mensagem_sucesso
            
    except Exception as e:
        return 0, 0, 0, f"Erro ao importar componentes: {e}"

def movimentar_lubrificante(id_lubrificante, tipo, quantidade, data, cod_equip=None, observacoes=""):
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO lubrificantes_movimentacoes (id_lubrificante, tipo, quantidade, data, cod_equip, observacoes) VALUES (?, ?, ?, ?, ?, ?)",
                (id_lubrificante, tipo, quantidade, data, cod_equip, observacoes)
            )
            # Atualiza estoque
            sinal = 1 if tipo == "entrada" else -1
            cur.execute(
                "UPDATE lubrificantes SET quantidade_estoque = quantidade_estoque + ? WHERE id = ?",
                (sinal * quantidade, id_lubrificante)
            )
            conn.commit()
        return True, "Movimentação registrada!"
    except Exception as e:
        return False, f"Erro: {e}"

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
        # Primeira tentativa: usar conexão direta
        conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        cursor = conn.cursor()
        
        # Converter tipos de dados para garantir compatibilidade
        cod_equip = int(cod_equip)  # Converter numpy.int64 para int
        titulo_checklist = str(titulo_checklist)
        data_preenchimento = str(data_preenchimento)
        turno = str(turno)
        
        # Debug: verificar todos os registros na tabela ANTES da exclusão
        cursor.execute("SELECT rowid, Cod_Equip, titulo_checklist, data_preenchimento, turno FROM checklist_historico")
        all_records_before = cursor.fetchall()
        
        # Tentar encontrar o registro com diferentes abordagens
        rowid = None
        
        # Primeira tentativa: busca exata
        cursor.execute(
            "SELECT rowid FROM checklist_historico WHERE Cod_Equip = ? AND titulo_checklist = ? AND data_preenchimento = ? AND turno = ?", 
            (cod_equip, titulo_checklist, data_preenchimento, turno)
        )
        result = cursor.fetchone()
        
        if result:
            rowid = result[0]
        else:
            # Segunda tentativa: buscar apenas por Cod_Equip, título e turno (ignorar data)
            cursor.execute(
                "SELECT rowid FROM checklist_historico WHERE Cod_Equip = ? AND titulo_checklist = ? AND turno = ?", 
                (cod_equip, titulo_checklist, turno)
            )
            result = cursor.fetchone()
            
            if result:
                rowid = result[0]
            else:
                # Terceira tentativa: buscar apenas por Cod_Equip e título
                cursor.execute(
                    "SELECT rowid FROM checklist_historico WHERE Cod_Equip = ? AND titulo_checklist = ?", 
                    (cod_equip, titulo_checklist)
                )
                result = cursor.fetchone()
                
                if result:
                    rowid = result[0]
        
        if rowid is None:
            # Debug: retornar informações sobre o que foi encontrado
            debug_info = f"""
            Registro não encontrado para exclusão.
            
            Valores procurados (após conversão):
            - Cod_Equip: {cod_equip} (tipo: {type(cod_equip)})
            - Título: {titulo_checklist} (tipo: {type(titulo_checklist)})
            - Data: {data_preenchimento} (tipo: {type(data_preenchimento)})
            - Turno: {turno} (tipo: {type(turno)})
            
            Todos os registros na tabela ANTES da exclusão:
            {all_records_before}
            """
            conn.close()
            return False, debug_info
        
        # Agora vamos excluir usando rowid
        cursor.execute("DELETE FROM checklist_historico WHERE rowid = ?", (rowid,))
        
        # Forçar commit imediato
        conn.commit()
        
        # Verificar se foi realmente excluído
        rows_deleted = cursor.rowcount
        if rows_deleted > 0:
            # Verificar novamente se o registro foi realmente excluído
            cursor.execute("SELECT COUNT(*) FROM checklist_historico WHERE rowid = ?", (rowid,))
            count_after = cursor.fetchone()[0]
            
            # Verificar também se o registro ainda existe pelos outros campos
            cursor.execute(
                "SELECT COUNT(*) FROM checklist_historico WHERE Cod_Equip = ? AND titulo_checklist = ? AND data_preenchimento = ? AND turno = ?", 
                (cod_equip, titulo_checklist, data_preenchimento, turno)
            )
            count_by_fields = cursor.fetchone()[0]
            
            if count_after == 0 and count_by_fields == 0:
                 # Verificar o total de registros na tabela
                 cursor.execute("SELECT COUNT(*) FROM checklist_historico")
                 total_after = cursor.fetchone()[0]
                 
                 # Forçar sincronização do banco
                 cursor.execute("PRAGMA wal_checkpoint(FULL)")
                 cursor.execute("PRAGMA synchronous=FULL")
                 conn.commit()
                 
                 success_msg = f"Checklist excluído com sucesso! ({rows_deleted} registro(s) removido(s)). Total na tabela: {total_after}"
                 
                 # Salvar backup automático para persistência no Streamlit Cloud
                 backup_success, backup_msg = save_backup_to_session_state()
                 if backup_success:
                     success_msg += f" | Backup salvo: {backup_msg}"
                 else:
                     success_msg += f" | Aviso: {backup_msg}"
                 
                 conn.close()
                 return True, success_msg
            else:
                conn.close()
                return False, f"Erro: Registro ainda existe após exclusão. Count by rowid: {count_after}, Count by fields: {count_by_fields}"
        else:
            conn.close()
            return False, "Nenhum registro foi excluído"
                
    except Exception as e:
        if 'conn' in locals():
            conn.close()
        return False, f"Erro ao excluir checklist: {e}"


def force_cache_clear():
    """Força a limpeza completa de todos os caches."""
    try:
        # Limpar cache de dados
        st.cache_data.clear()
        
        # Limpar cache de recursos
        st.cache_resource.clear()
        
        # Forçar rerun da aplicação
        st.rerun()
    except Exception as e:
        st.error(f"Erro ao limpar cache: {e}")


def force_database_sync():
    """Força a sincronização do banco de dados com o disco."""
    try:
        conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        cursor = conn.cursor()
        
        # Forçar commit
        conn.commit()
        
        # Executar PRAGMA para forçar sincronização
        cursor.execute("PRAGMA wal_checkpoint(FULL)")
        cursor.execute("PRAGMA synchronous=FULL")
        cursor.execute("PRAGMA journal_mode=DELETE")
        
        # Forçar commit novamente
        conn.commit()
        
        # Verificar se o banco está em modo WAL
        cursor.execute("PRAGMA journal_mode")
        journal_mode = cursor.fetchone()[0]
        
        conn.close()
        
        return True, f"Banco sincronizado. Modo journal: {journal_mode}"
    except Exception as e:
        return False, f"Erro ao sincronizar banco: {e}"


def export_database_backup():
    """Exporta todos os dados do banco para um arquivo de backup."""
    try:
        conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        
        # Obter todas as tabelas
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        
        backup_data = {}
        
        for table in tables:
            table_name = table[0]
            if table_name != 'sqlite_master':
                # Exportar dados da tabela
                df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
                backup_data[table_name] = df.to_dict('records')
        
        conn.close()
        
        # Converter para JSON
        backup_json = json.dumps(backup_data, default=str, indent=2)
        
        # Criar arquivo de download
        backup_bytes = backup_json.encode('utf-8')
        backup_b64 = base64.b64encode(backup_bytes).decode()
        
        return backup_b64, backup_data
        
    except Exception as e:
        return None, f"Erro ao exportar backup: {e}"


def import_database_backup(backup_data):
    """Importa dados de backup para o banco."""
    try:
        conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        cursor = conn.cursor()
        
        for table_name, records in backup_data.items():
            if records:  # Se a tabela tem dados
                # Limpar tabela existente
                cursor.execute(f"DELETE FROM {table_name}")
                
                # Inserir novos dados
                for record in records:
                    columns = list(record.keys())
                    placeholders = ', '.join(['?' for _ in columns])
                    values = list(record.values())
                    
                    # Converter tipos de dados
                    converted_values = []
                    for value in values:
                        if isinstance(value, str):
                            # Tentar converter para datetime se for uma data
                            try:
                                if 'T' in value or '-' in value:
                                    dt = pd.to_datetime(value)
                                    converted_values.append(dt.strftime('%Y-%m-%d %H:%M:%S'))
                                else:
                                    converted_values.append(value)
                            except:
                                converted_values.append(value)
                        else:
                            converted_values.append(value)
                    
                    cursor.execute(
                        f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})",
                        converted_values
                    )
        
        conn.commit()
        conn.close()
        
        return True, "Backup restaurado com sucesso!"
        
    except Exception as e:
        return False, f"Erro ao restaurar backup: {e}"


def save_backup_to_session_state():
    """Salva backup dos dados na sessão do Streamlit."""
    try:
        backup_b64, backup_data = export_database_backup()
        if backup_b64:
            st.session_state['database_backup'] = backup_b64
            st.session_state['backup_timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            return True, "Backup salvo na sessão"
        else:
            return False, "Erro ao criar backup"
    except Exception as e:
        return False, f"Erro ao salvar backup: {e}"


def restore_backup_from_session_state():
    """Restaura backup dos dados da sessão do Streamlit."""
    try:
        if 'database_backup' in st.session_state:
            backup_b64 = st.session_state['database_backup']
            backup_bytes = base64.b64decode(backup_b64)
            backup_json = backup_bytes.decode('utf-8')
            backup_data = json.loads(backup_json)
            
            success, message = import_database_backup(backup_data)
            if success:
                # Limpar cache para forçar recarregamento
                force_cache_clear()
                return True, message
            else:
                return False, message
        else:
            return False, "Nenhum backup encontrado na sessão"
    except Exception as e:
        return False, f"Erro ao restaurar backup: {e}"


def auto_restore_backup_on_startup():
    """Tenta restaurar backup automaticamente na inicialização da aplicação."""
    try:
        if 'database_backup' in st.session_state:
            # Verificar se o banco está vazio
            conn = sqlite3.connect(DB_PATH, check_same_thread=False)
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table'")
            num_tables = cursor.fetchone()[0]
            conn.close()
            
            if num_tables == 0:
                # Banco vazio, tentar restaurar
                success, message = restore_backup_from_session_state()
                if success:
                    st.info("🔄 Backup restaurado automaticamente na inicialização!")
                    return True
                else:
                    st.warning(f"⚠️ Falha na restauração automática: {message}")
                    return False
        return False
    except Exception as e:
        st.warning(f"⚠️ Erro na restauração automática: {e}")
        return False


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
            col_logo, col_title = st.columns([1, 8])
            with col_logo:
                st.image("logo.png", width=80)
            with col_title:
                st.title("📊 Dashboard de Frotas e Abastecimentos")
        else:
            st.title("📊 Dashboard de Frotas e Abastecimentos")

        # Tentar restaurar backup automaticamente na inicialização
        auto_restore_backup_on_startup()
        
        # Adicionar coluna de tipo de combustível se não existir
        add_tipo_combustivel_column()
        
        # Setup de esquemas (motoristas, preços, combustível)
        ensure_motoristas_schema()
        ensure_precos_combustivel_schema()

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
                st.image("logo.png", width=200)
            st.write(f"Bem-vindo, **{st.session_state.username}**!")
            if st.button("Sair"):
                st.session_state.authenticated = False
                st.session_state.username = "" # Limpa o username ao sair
                st.session_state.role = None
                st.rerun()
            st.markdown("---")

        with st.sidebar:
            st.header("📅 Filtros (válidos apenas na aba Análise Geral)")

            # Persistência de período
            if 'filtro_data_inicio' not in st.session_state:
                st.session_state['filtro_data_inicio'] = df['Data'].min().date()
            if 'filtro_data_fim' not in st.session_state:
                st.session_state['filtro_data_fim'] = df['Data'].max().date()

            st.subheader("Período de Análise")
            data_inicio = st.date_input(
                "Data de Início", 
                st.session_state['filtro_data_inicio'],
                key='data_inicio'
            )
            data_fim = st.date_input(
                "Data de Fim", 
                st.session_state['filtro_data_fim'],
                key='data_fim'
            )
            st.session_state['filtro_data_inicio'] = data_inicio
            st.session_state['filtro_data_fim'] = data_fim

            st.markdown("---")
            st.caption("Desenvolvido por André Luis")

            with st.expander("Filtrar por Classe Operacional"):
                classe_opts = sorted(list(df["Classe_Operacional"].dropna().unique()))
                sel_classes = st.multiselect(
                    "Selecione as Classes", 
                    classe_opts, 
                    default=classe_opts,
                    key="sel_classes"
                )

            with st.expander("Filtrar por Safra"):
                safra_opts = sorted(list(df["Safra"].dropna().unique()))
                sel_safras = st.multiselect(
                    "Selecione as Safras", 
                    safra_opts, 
                    default=safra_opts,
                    key="sel_safras"
                )

            # Só aplicaremos os filtros na aba "📈 Análise Geral" (guardaremos em sessão)
            st.session_state['filtro_opts_analise'] = {
                "data_inicio": data_inicio,
                "data_fim": data_fim,
                "classes_op": sel_classes,
                "safras": sel_safras
            }
    #----------------------------------------------------- aba principal --------------------------------------
        # df_f será calculado apenas para a aba Análise Geral
        df_f = None
        plan_df = build_component_maintenance_plan(df_frotas, df, df_comp_regras, df_comp_historico)


        # CSS para barra de rolagem horizontal nas abas com design moderno
        st.markdown("""
        <style>
        .stTabs [data-baseweb="tab-list"] {
            gap: 8px;
            overflow-x: auto;
            scrollbar-width: thin;
            scrollbar-color: #00D4AA #E8F5F2;
            padding: 8px 0;
        }
        
        .stTabs [data-baseweb="tab-list"]::-webkit-scrollbar {
            height: 12px;
        }
        
        .stTabs [data-baseweb="tab-list"]::-webkit-scrollbar-track {
            background: linear-gradient(90deg, #F0F2F6 0%, #E8F5F2 100%);
            border-radius: 8px;
            border: 1px solid #E0E6ED;
            box-shadow: inset 0 1px 3px rgba(0,0,0,0.1);
        }
        
        .stTabs [data-baseweb="tab-list"]::-webkit-scrollbar-thumb {
            background: linear-gradient(90deg, #00D4AA 0%, #00B8A9 100%);
            border-radius: 8px;
            border: 1px solid #00A896;
            box-shadow: 0 2px 4px rgba(0,212,170,0.3);
            transition: all 0.3s ease;
        }
        
        .stTabs [data-baseweb="tab-list"]::-webkit-scrollbar-thumb:hover {
            background: linear-gradient(90deg, #00B8A9 0%, #00A896 100%);
            box-shadow: 0 3px 6px rgba(0,212,170,0.4);
            transform: translateY(-1px);
        }
        
        .stTabs [data-baseweb="tab-list"]::-webkit-scrollbar-thumb:active {
            background: linear-gradient(90deg, #00A896 0%, #009688 100%);
            box-shadow: 0 1px 3px rgba(0,212,170,0.5);
        }
        
        .stTabs [data-baseweb="tab-list"] > div {
            flex-shrink: 0;
            transition: all 0.2s ease;
        }
        
        /* Melhorar aparência das abas */
        .stTabs [data-baseweb="tab"] {
            border-radius: 8px;
            transition: all 0.2s ease;
        }
        
        .stTabs [data-baseweb="tab"]:hover {
            transform: translateY(-1px);
            box-shadow: 0 2px 8px rgba(0,0,0,0.1);
        }
        
        /* Estilo para abas ativas */
        .stTabs [data-baseweb="tab"][aria-selected="true"] {
            background: linear-gradient(135deg, #00D4AA 0%, #00B8A9 100%);
            color: white;
            font-weight: 600;
            box-shadow: 0 4px 12px rgba(0,212,170,0.3);
        }
        </style>
        """, unsafe_allow_html=True)
        
        # Definição dos grupos de abas
        abas_pagina_inicial = ["📈 Análise Geral", "🛠️ Controle de Manutenção", "🔎 Consulta Individual", "✅ Checklists Diários"]
        abas_gerir = ["⚙️ Gerir Lançamentos", "🛢️ Gestão de Lubrificantes", "⚙️ Gerir Frotas", "✅ Gerir Checklists"]
        abas_dados = ["📤 Importar Dados", "⚕️ Saúde dos Dados", "💾 Backup", "👤 Gerir Utilizadores", "⚙️ Configurações"]

        # Sistema de navegação por grupos
        st.markdown("### 🎯 Navegação por Grupos")
        
        # Mostrar informações sobre os grupos disponíveis
        if st.session_state.role == 'admin':
            st.info("""
            **🏠 Página Inicial:** Visualizações, análises e consultas principais | 
            **⚙️ Gerir:** Gestão de lançamentos, frotas e checklists | 
            **📊 Dados:** Importações, backup e saúde dos dados
            """)
        else:
            st.info("**🏠 Página Inicial:** Visualizações, análises e consultas principais")
        
        # CSS para os botões de grupo
        st.markdown("""
        <style>
        .group-nav-button {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            border: none;
            border-radius: 12px;
            padding: 12px 20px;
            font-weight: 600;
            font-size: 14px;
            transition: all 0.3s ease;
            box-shadow: 0 4px 12px rgba(102,126,234,0.3);
            margin: 4px;
            cursor: pointer;
        }
        
        .group-nav-button:hover {
            background: linear-gradient(135deg, #764ba2 0%, #667eea 100%);
            transform: translateY(-2px);
            box-shadow: 0 6px 16px rgba(102,126,234,0.4);
        }
        
        .group-nav-button.active {
            background: linear-gradient(135deg, #00D4AA 0%, #00B8A9 100%);
            box-shadow: 0 4px 12px rgba(0,212,170,0.3);
        }
        
        .group-section {
            background: linear-gradient(135deg, #f8f9fa 0%, #e9ecef 100%);
            border-radius: 12px;
            padding: 16px;
            margin: 8px 0;
            border: 1px solid #dee2e6;
        }
        </style>
        """, unsafe_allow_html=True)

        # Botões de navegação por grupos
        col1, col2, col3 = st.columns(3)
        
        # Determinar grupo ativo
        active_group = st.session_state.get('active_group', 'pagina_inicial').strip().lower()

        with col1:
            if st.button("🏠 Página Inicial", key="nav_pagina_inicial", help="Visualizações e análises principais"):
                st.session_state['active_group'] = 'pagina_inicial'
                st.session_state['active_tab_index'] = 0
                st.rerun()
        
        if st.session_state.role == 'admin':
            with col2:
                if st.button("⚙️ Gerir", key="nav_gerir", help="Gestão de lançamentos, frotas e checklists"):
                    st.session_state['active_group'] = 'gerir'
                    st.session_state['active_tab_index'] = 0
                    st.rerun()
            
            with col3:
                if st.button("📊 Dados", key="nav_dados", help="Importações, backup e saúde dos dados"):
                    st.session_state['active_group'] = 'dados'
                    st.session_state['active_tab_index'] = 0
                    st.rerun()

        # Determinar quais abas mostrar baseado no grupo ativo
        if st.session_state.role == 'admin':
            if active_group == 'pagina_inicial':
                tabs_para_mostrar = abas_pagina_inicial
            elif active_group == 'gerir':
                tabs_para_mostrar = abas_gerir
            elif active_group == 'dados':
                tabs_para_mostrar = abas_dados
            else:
                tabs_para_mostrar = abas_pagina_inicial
        else:
            # Para usuários comuns, mostrar apenas página inicial
            tabs_para_mostrar = abas_pagina_inicial

        # Determinar índice ativo
        active_idx = st.session_state.get('active_tab_index', 0)
        active_idx = max(0, min(active_idx, len(tabs_para_mostrar) - 1))
        
        # Criar as abas
        try:
            abas = st.tabs(tabs_para_mostrar, default_index=active_idx)
        except TypeError:
            abas = st.tabs(tabs_para_mostrar)

        # Atribuir as abas baseado no grupo ativo
        if st.session_state.role == 'admin':
            if active_group == 'pagina_inicial':
                tab_analise, tab_manut, tab_consulta, tab_checklists = abas
                # Criar variáveis vazias para as outras abas
                tab_gerir_lanc = tab_gerir_lub = tab_gerir_frotas = tab_gerir_checklists = None
                tab_importar = tab_saude = tab_backup = tab_gerir_users = tab_config = None
            elif active_group == 'gerir':
                tab_gerir_lanc, tab_gerir_lub, tab_gerir_frotas, tab_gerir_checklists = abas
                # Criar variáveis vazias para as outras abas
                tab_analise = tab_manut = tab_consulta = tab_checklists = None
                tab_importar = tab_saude = tab_backup = tab_gerir_users = tab_config = None
            elif active_group == 'dados':
                tab_importar, tab_saude, tab_backup, tab_gerir_users, tab_config = abas
                # Criar variáveis vazias para as outras abas
                tab_analise = tab_manut = tab_consulta = tab_checklists = None
                tab_gerir_lanc = tab_gerir_lub = tab_gerir_frotas = tab_gerir_checklists = None
                

        else:
            tab_analise, tab_manut, tab_consulta, tab_checklists = abas
            # Criar variáveis vazias para as outras abas
            tab_gerir_lanc = tab_gerir_lub = tab_gerir_frotas = tab_gerir_checklists = None
            tab_importar = tab_saude = tab_backup = tab_gerir_users = tab_config = None

        def rerun_keep_tab(tab_title: str, clear_cache: bool = True):
            if clear_cache:
                st.cache_data.clear()
            try:
                st.session_state['active_tab_index'] = tabs_para_mostrar.index(tab_title)
            except Exception:
                pass
            st.rerun()
    
        if tab_analise is not None:
            with tab_analise:
                st.header("📈 Análise Geral e Painel de Controle")
                
                # ===== SEÇÃO: VISÃO GERAL DA FROTA =====
                st.subheader("🏠 Visão Geral da Frota")
                
                # Calcular gasto total com combustível
                precos_map = get_precos_combustivel_map()
                gasto_total_combustivel = 0
                if precos_map:
                    df_gastos_total = df.copy()
                    # Verificar se a coluna tipo_combustivel existe em df_frotas
                    if 'tipo_combustivel' in df_frotas.columns:
                        df_gastos_total = df_gastos_total.merge(df_frotas[['Cod_Equip','tipo_combustivel']], on='Cod_Equip', how='left')
                        # Verificar se a coluna foi criada após o merge
                        if 'tipo_combustivel' in df_gastos_total.columns:
                            df_gastos_total['tipo_combustivel'] = df_gastos_total['tipo_combustivel'].fillna('Diesel S500')
                        else:
                            df_gastos_total['tipo_combustivel'] = 'Diesel S500'
                    else:
                        # Se não existir, criar a coluna com valor padrão
                        df_gastos_total['tipo_combustivel'] = 'Diesel S500'
                    
                    df_gastos_total['preco_unit'] = df_gastos_total['tipo_combustivel'].map(precos_map).fillna(0.0)
                    df_gastos_total['custo'] = df_gastos_total['Qtde_Litros'].fillna(0.0) * df_gastos_total['preco_unit']
                    gasto_total_combustivel = df_gastos_total['custo'].sum()
                
                # KPIs principais
                kpi1, kpi2, kpi3, kpi4, kpi5 = st.columns(5)
                
                # KPI 1: Frotas Ativas
                total_frotas_ativas = df_frotas[df_frotas['ATIVO'] == 'ATIVO']['Cod_Equip'].nunique()
                kpi1.metric("🚗 Frotas Ativas", total_frotas_ativas)
                
                # KPI 2: Frotas com Alerta
                frotas_com_alerta = plan_df[plan_df['Qualquer_Alerta'] == True]['Cod_Equip'].nunique() if not plan_df.empty else 0
                kpi2.metric("⚠️ Frotas com Alerta", frotas_com_alerta)
                
                # KPI 3: Gasto Total com Combustível
                kpi3.metric("💰 Gasto com Combustível", formatar_brasileiro(gasto_total_combustivel, 'R$ '))
                
                # KPIs 4 e 5: Frotas Mais e Menos Eficientes
                df_sem_filtro = df.copy()
                df_media_geral = df_sem_filtro[(df_sem_filtro['Media'].notna()) & (df_sem_filtro['Media'] > 0)]
                if not df_media_geral.empty:
                    # Agrupa por Código e Descrição para ter acesso a ambos
                    media_por_equip = df_media_geral.groupby(['Cod_Equip', 'DESCRICAO_EQUIPAMENTO'])['Media'].mean().sort_values()
                    
                    if not media_por_equip.empty:
                        # Pega o CÓDIGO do mais eficiente (primeiro da lista ordenada)
                        cod_mais_eficiente = media_por_equip.index[0][0]
                        media_mais_eficiente = media_por_equip.iloc[0]
                        # Exibe o CÓDIGO no KPI
                        kpi4.metric("🏆 Mais Eficiente", f"{cod_mais_eficiente}", f"{formatar_brasileiro(media_mais_eficiente)}")
                
                        # Pega o CÓDIGO do menos eficiente (último da lista ordenada)
                        cod_menos_eficiente = media_por_equip.index[-1][0]
                        media_menos_eficiente = media_por_equip.iloc[-1]
                        # Exibe o CÓDIGO no KPI
                        kpi5.metric("📉 Menos Eficiente", f"{cod_menos_eficiente}", f"{formatar_brasileiro(media_menos_eficiente)}")
                else:
                    # Se não há dados de eficiência, mostrar mensagem
                    kpi4.metric("🏆 Mais Eficiente", "N/A")
                    kpi5.metric("📉 Menos Eficiente", "N/A")

                st.markdown("---")
                st.subheader("📈 Análise Gráfica de Consumo")

                # Aplica filtros apenas nesta aba
                opts = st.session_state.get('filtro_opts_analise', None)
                df_f = filtrar_dados(df, opts) if opts else df.copy()

                if not df_f.empty:
                    if 'Media' in df_f.columns:
                        k1, k2 = st.columns(2)
                        k1.metric("Litros Consumidos (período)", formatar_brasileiro_int(df_f["Qtde_Litros"].sum()))
                        k2.metric("Média Consumo (período)", f"{formatar_brasileiro(df_f['Media'].mean())}")
                    else:
                        k1.metric("Litros Consumidos (período)", formatar_brasileiro_int(df_f["Qtde_Litros"].sum()))
                    st.markdown("---")
                    st.subheader("📊 Análise de Consumo por Classe e Equipamentos")
                    c1, c2 = st.columns(2)

                    with c1:
                        st.subheader("Consumo por Classe Operacional")
                        classes_a_excluir = ['VEICULOS LEVES', 'MOTOCICLETA', 'MINI CARREGADEIRA', 'USINA']
                        # Verificar se a coluna Classe_Operacional existe antes de filtrar
                        if 'Classe_Operacional' in df_f.columns:
                            df_consumo_classe = df_f[~df_f['Classe_Operacional'].str.upper().isin(classes_a_excluir)]
                        else:
                            df_consumo_classe = df_f
                        consumo_por_classe = df_consumo_classe.groupby("Classe_Operacional")["Qtde_Litros"].sum().sort_values(ascending=False).reset_index()

                        if not consumo_por_classe.empty:
                            consumo_por_classe['texto_formatado'] = consumo_por_classe['Qtde_Litros'].apply(formatar_brasileiro_int)
                            fig_classe = px.bar(consumo_por_classe, x='Qtde_Litros', y='Classe_Operacional', orientation='h', text='texto_formatado', labels={"x": "Litros Consumidos", "y": "Classe Operacional"})
                            fig_classe.update_traces(texttemplate='%{text} L', textposition='outside')
                            fig_classe.update_layout(yaxis={'categoryorder':'total ascending'}, xaxis_title="Total Consumido (Litros)", yaxis_title="Classe Operacional")
                            st.plotly_chart(fig_classe, use_container_width=True)

                    with c2:
                        st.subheader("Top 10 Equipamentos com Maior Consumo")
                        # Melhorar o gráfico com informações mais claras
                        consumo_por_equip = df_f.groupby("Cod_Equip").agg({'Qtde_Litros': 'sum'}).dropna()
                        consumo_por_equip = consumo_por_equip[consumo_por_equip.index != 550]
                        consumo_por_equip = consumo_por_equip.sort_values(by="Qtde_Litros", ascending=False).head(10)

                        if not consumo_por_equip.empty:
                            # Adicionar informações da frota para melhor identificação
                            consumo_por_equip = consumo_por_equip.reset_index()
                            consumo_por_equip = consumo_por_equip.merge(
                                df_frotas[['Cod_Equip', 'DESCRICAO_EQUIPAMENTO', 'PLACA']], 
                                on='Cod_Equip', 
                                how='left'
                            )
                            
                            # Criar label mais informativo: Código - Descrição (Placa)
                            consumo_por_equip['label_grafico'] = consumo_por_equip.apply(
                                lambda row: f"{row['Cod_Equip']} - {row['DESCRICAO_EQUIPAMENTO'][:20]}{'...' if len(str(row['DESCRICAO_EQUIPAMENTO'])) > 20 else ''} ({row['PLACA']})", 
                                axis=1
                            )
                            
                            consumo_por_equip['texto_formatado'] = consumo_por_equip['Qtde_Litros'].apply(formatar_brasileiro_int)
                            
                            fig_top10 = px.bar(
                                consumo_por_equip, 
                                x='Qtde_Litros', 
                                y='label_grafico', 
                                orientation='h', 
                                text='texto_formatado', 
                                labels={"Qtde_Litros": "Total Consumido (Litros)", "label_grafico": "Equipamento"},
                                title="Top 10 Equipamentos com Maior Consumo"
                            )
                            fig_top10.update_traces(
                                texttemplate='%{text} L', 
                                textposition='outside',
                                marker_color='#ff7f0e'
                            )
                            fig_top10.update_layout(
                                yaxis={'categoryorder':'total ascending'}, 
                                xaxis_title="Total Consumido (Litros)", 
                                yaxis_title="Equipamento",
                                height=400
                            )
                            st.plotly_chart(fig_top10, use_container_width=True)

                    st.markdown("---")
                    
                    # NOVA SEÇÃO: Top 10 de Gastos por Frota e por Classe
                    st.subheader("💰 Top 10 de Gastos por Frota e Classe")
                    
                    # Calcular gastos por frota
                    precos_map = get_precos_combustivel_map()
                    if precos_map:
                        df_gastos = df_f.copy()
                        
                        # Verificar se a coluna tipo_combustivel existe em df_frotas
                        if 'tipo_combustivel' in df_frotas.columns:
                            df_gastos = df_gastos.merge(df_frotas[['Cod_Equip','tipo_combustivel']], on='Cod_Equip', how='left')
                            # Verificar se a coluna foi criada após o merge
                            if 'tipo_combustivel' in df_gastos.columns:
                                df_gastos['tipo_combustivel'] = df_gastos['tipo_combustivel'].fillna('Diesel S500')
                            else:
                                df_gastos['tipo_combustivel'] = 'Diesel S500'
                        else:
                            # Se não existir, criar a coluna com valor padrão
                            df_gastos['tipo_combustivel'] = 'Diesel S500'
                        
                        # Garantir que a coluna tipo_combustivel existe antes de mapear preços
                        if 'tipo_combustivel' not in df_gastos.columns:
                            df_gastos['tipo_combustivel'] = 'Diesel S500'
                        
                        df_gastos['preco_unit'] = df_gastos['tipo_combustivel'].map(precos_map).fillna(0.0)
                        df_gastos['custo'] = df_gastos['Qtde_Litros'].fillna(0.0) * df_gastos['preco_unit']
                        
                        # Adicionar informações da frota para filtro
                        df_gastos_com_info = df_gastos.merge(
                            df_frotas[['Cod_Equip', 'DESCRICAO_EQUIPAMENTO', 'PLACA', 'Classe_Operacional']], 
                            on='Cod_Equip', 
                            how='left'
                        )
                        
                        # Garantir que a coluna Classe_Operacional existe
                        if 'Classe_Operacional' not in df_gastos_com_info.columns:
                            df_gastos_com_info['Classe_Operacional'] = 'N/A'
                        
                        # Filtro para excluir a frota 550 (usina) por padrão
                        mostrar_usinas = st.checkbox("🏭 Incluir Frota 550 (Usina) no Top 10 de Gastos por Frota", value=False)
                        
                        if not mostrar_usinas:
                            # Excluir a frota 550 (usina) do DataFrame
                            df_gastos_filtrado = df_gastos_com_info[df_gastos_com_info['Cod_Equip'] != 550]
                        else:
                            df_gastos_filtrado = df_gastos_com_info
                        
                        # Top 10 gastos por frota individual (após filtro)
                        gastos_por_frota = df_gastos_filtrado.groupby('Cod_Equip').agg({
                            'custo': 'sum',
                            'Qtde_Litros': 'sum'
                        }).sort_values('custo', ascending=False).head(10).reset_index()
                        
                        # Adicionar informações da frota
                        gastos_por_frota = gastos_por_frota.merge(
                            df_frotas[['Cod_Equip', 'DESCRICAO_EQUIPAMENTO', 'PLACA']], 
                            on='Cod_Equip', 
                            how='left'
                        )
                        gastos_por_frota['label_frota'] = gastos_por_frota.apply(
                            lambda row: f"{row['Cod_Equip']} - {row['DESCRICAO_EQUIPAMENTO'][:15]}{'...' if len(str(row['DESCRICAO_EQUIPAMENTO'])) > 15 else ''}", 
                            axis=1
                        )
                        gastos_por_frota['custo_formatado'] = gastos_por_frota['custo'].apply(lambda x: formatar_brasileiro(x, 'R$ '))
                        
                        # Top 10 gastos por classe operacional
                        gastos_por_classe = df_gastos.groupby('Classe_Operacional').agg({
                            'custo': 'sum',
                            'Qtde_Litros': 'sum'
                        }).sort_values('custo', ascending=False).head(10).reset_index()
                        gastos_por_classe['custo_formatado'] = gastos_por_classe['custo'].apply(lambda x: formatar_brasileiro(x, 'R$ '))
                        
                        # Criar layout em 2 colunas para os gráficos
                        col_gastos1, col_gastos2 = st.columns(2)
                        
                        with col_gastos1:
                            st.subheader("🏭 Top 10 Gastos por Frota")
                            
                            # Mostrar informação sobre filtro da frota 550
                            # Comentário removido para manter proporção dos gráficos
                            
                            if not gastos_por_frota.empty:
                                fig_gastos_frota = px.bar(
                                    gastos_por_frota,
                                    x='custo',
                                    y='label_frota',
                                    orientation='h',
                                    text='custo_formatado',
                                    title="Gastos por Frota Individual",
                                    labels={'custo': 'Custo (R$)', 'label_frota': 'Frota'},
                                    color='custo',
                                    color_continuous_scale='Reds'
                                )
                                fig_gastos_frota.update_traces(
                                    textposition='outside',
                                    texttemplate='%{text}'
                                )
                                fig_gastos_frota.update_layout(
                                    yaxis={'categoryorder':'total ascending'},
                                    xaxis_title="Custo Total (R$)",
                                    yaxis_title="Frota",
                                    height=400,
                                    showlegend=False
                                )
                                st.plotly_chart(fig_gastos_frota, use_container_width=True)
                            else:
                                st.info("Não há dados de gastos por frota.")
                        
                        with col_gastos2:
                            st.subheader("🏗️ Top 10 Gastos por Classe")
                            if not gastos_por_classe.empty:
                                fig_gastos_classe = px.bar(
                                    gastos_por_classe,
                                    x='custo',
                                    y='Classe_Operacional',
                                    orientation='h',
                                    text='custo_formatado',
                                    title="Gastos por Classe Operacional",
                                    labels={'custo': 'Custo (R$)', 'Classe_Operacional': 'Classe'},
                                    color='custo',
                                    color_continuous_scale='Blues'
                                )
                                fig_gastos_classe.update_traces(
                                    textposition='outside',
                                    texttemplate='%{text}'
                                )
                                fig_gastos_classe.update_layout(
                                    yaxis={'categoryorder':'total ascending'},
                                    xaxis_title="Custo Total (R$)",
                                    yaxis_title="Classe Operacional",
                                    height=400,
                                    showlegend=False
                                )
                                st.plotly_chart(fig_gastos_classe, use_container_width=True)
                            else:
                                st.info("Não há dados de gastos por classe.")
                        
                        # Resumo dos totais
                        st.markdown("---")
                        col_resumo1, col_resumo2, col_resumo3 = st.columns(3)
                        with col_resumo1:
                            st.metric(
                                "Total Gastos (Período)", 
                                formatar_brasileiro(df_gastos['custo'].sum(), 'R$ ')
                            )
                        with col_resumo2:
                            if not gastos_por_frota.empty:
                                frota_maior_gasto = gastos_por_frota.iloc[0]
                                st.metric(
                                    "Frota com Maior Gasto", 
                                    f"{frota_maior_gasto['Cod_Equip']}",
                                    f"{frota_maior_gasto['custo_formatado']}"
                                )
                            else:
                                st.metric("Frota com Maior Gasto", "N/A")
                        with col_resumo3:
                            st.metric(
                                "Classe com Maior Gasto", 
                                f"{gastos_por_classe.iloc[0]['Classe_Operacional'] if not gastos_por_classe.empty else 'N/A'}"
                            )
                    else:
                        st.warning("Cadastre os preços de combustível na aba Importar > Preços para visualizar os gastos.")

                    st.markdown("---")
                    st.subheader("📈 Média de Consumo por Classe Operacional")
                    df_media = df_f[(df_f['Media'].notna()) & (df_f['Media'] > 0)].copy()

                    classes_para_excluir = ['MOTOCICLETA', 'VEICULOS LEVES', 'USINA', 'MINI CARREGADEIRA']

                    # Verificar se a coluna Classe_Operacional existe antes de filtrar
                    if 'Classe_Operacional' in df_media.columns:
                        df_media_filtrado = df_media[~df_media['Classe_Operacional'].str.upper().isin(classes_para_excluir)]
                    else:
                        df_media_filtrado = df_media

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

                    st.markdown("---")
                    st.subheader("💰 Total de Gasto por Motorista")
                    precos_map = get_precos_combustivel_map()
                    if precos_map:
                        # Vincula combustível por frota e multiplica litros por preço
                        df_tmp = df_f.copy()
                        
                        # Verificar se a coluna tipo_combustivel existe em df_frotas
                        if 'tipo_combustivel' in df_frotas.columns:
                            df_tmp = df_tmp.merge(df_frotas[['Cod_Equip','tipo_combustivel']], on='Cod_Equip', how='left')
                            # Verificar se a coluna foi criada após o merge
                            if 'tipo_combustivel' in df_tmp.columns:
                                df_tmp['tipo_combustivel'] = df_tmp['tipo_combustivel'].fillna('Diesel S500')
                            else:
                                df_tmp['tipo_combustivel'] = 'Diesel S500'
                        else:
                            # Se não existir, criar a coluna com valor padrão
                            df_tmp['tipo_combustivel'] = 'Diesel S500'
                        # Garantir que a coluna tipo_combustivel existe antes de mapear preços
                        if 'tipo_combustivel' not in df_tmp.columns:
                            df_tmp['tipo_combustivel'] = 'Diesel S500'
                        
                        df_tmp['preco_unit'] = df_tmp['tipo_combustivel'].map(precos_map).fillna(0.0)
                        df_tmp['custo'] = df_tmp['Qtde_Litros'].fillna(0.0) * df_tmp['preco_unit']
                        # Agrupar por matrícula
                        if 'Matricula' in df_tmp.columns:
                            gasto_motorista = df_tmp.groupby('Matricula').agg({'custo':'sum', 'Qtde_Litros':'sum'}).sort_values('custo', ascending=False)
                            gasto_motorista = gasto_motorista[gasto_motorista['custo']>0]
                            if not gasto_motorista.empty:
                                gasto_motorista = gasto_motorista.reset_index()
                                gasto_motorista['Custo (R$)'] = gasto_motorista['custo'].apply(lambda x: formatar_brasileiro(x, 'R$ '))
                                gasto_motorista['Litros'] = gasto_motorista['Qtde_Litros'].apply(formatar_brasileiro_int)
                                st.dataframe(gasto_motorista[['Matricula','Litros','Custo (R$)']])
                                try:
                                    fig_gasto = px.bar(gasto_motorista.head(10), x='custo', y='Matricula', orientation='h', text='Custo (R$)', labels={'custo':'Custo (R$)','Matricula':'Matrícula'})
                                    st.plotly_chart(fig_gasto, use_container_width=True)
                                except Exception:
                                    pass
                            else:
                                st.info("Sem dados suficientes de custo (verifique preços cadastrados).")
                        else:
                            st.info("Não há coluna de matrícula nos abastecimentos para calcular o gasto por motorista.")
                    else:
                        st.info("Cadastre os preços de combustível na aba Importar > Preços.")

                    st.markdown("---")
                    st.subheader("🔄 Análise de Proporções por Classe e Combustível")
                
                # Criar DataFrame com informações de combustível
                df_consumo_combustivel = df_f.copy()
                
                # Verificar se a coluna tipo_combustivel existe em df_frotas
                if 'tipo_combustivel' in df_frotas.columns:
                    try:
                        frotas_combustivel = df_frotas[['Cod_Equip', 'tipo_combustivel']].copy()
                        frotas_combustivel['tipo_combustivel'] = frotas_combustivel['tipo_combustivel'].fillna('Diesel S500')
                        df_consumo_combustivel = df_consumo_combustivel.merge(
                            frotas_combustivel, 
                            on='Cod_Equip', 
                            how='left'
                        )
                        # Verificar se a coluna foi criada após o merge
                        if 'tipo_combustivel' not in df_consumo_combustivel.columns:
                            df_consumo_combustivel['tipo_combustivel'] = 'Diesel S500'
                    except Exception:
                        df_consumo_combustivel['tipo_combustivel'] = 'Diesel S500'
                else:
                    df_consumo_combustivel['tipo_combustivel'] = 'Diesel S500'
                
                # Garantir que a coluna tipo_combustivel existe
                if 'tipo_combustivel' not in df_consumo_combustivel.columns:
                    df_consumo_combustivel['tipo_combustivel'] = 'Diesel S500'
                
                col_grafico1, col_grafico2 = st.columns(2)
                
                with col_grafico1:
                    st.subheader("📊 Consumo por Classe (Visão Macro)")
                    # Excluir "Usina" e frotas sem classe, usar Classe_Operacional
                    classes_a_excluir_macro = ['USINA', 'USINA MOBILE', 'USINA FIXA']
                    # Verificar se a coluna Classe_Operacional existe antes de filtrar
                    if 'Classe_Operacional' in df_consumo_combustivel.columns:
                        df_consumo_classe_macro = df_consumo_combustivel[
                            (df_consumo_combustivel['Classe_Operacional'].notna()) & 
                            (~df_consumo_combustivel['Classe_Operacional'].str.upper().isin(classes_a_excluir_macro))
                        ]
                    else:
                        df_consumo_classe_macro = df_consumo_combustivel
                    
                    if not df_consumo_classe_macro.empty:
                        try:
                            consumo_por_classe_macro = df_consumo_classe_macro.groupby("Classe_Operacional")["Qtde_Litros"].sum().sort_values(ascending=False).reset_index()
                            
                            # Criar gráfico de pizza
                            fig_pizza_classe = px.pie(
                                consumo_por_classe_macro, 
                                values='Qtde_Litros', 
                                names='Classe_Operacional',
                                title="Proporção de Consumo por Classe",
                                hole=0.3
                            )
                            fig_pizza_classe.update_traces(textposition='inside', textinfo='percent+label')
                            fig_pizza_classe.update_layout(height=400)
                            st.plotly_chart(fig_pizza_classe, use_container_width=True)
                            
                            # Mostrar totais
                            st.info(f"**Total de classes analisadas:** {len(consumo_por_classe_macro)}")
                            st.info(f"**Total de litros consumidos:** {formatar_brasileiro_int(consumo_por_classe_macro['Qtde_Litros'].sum())} L")
                        except Exception as e:
                            st.error(f"Erro ao criar gráfico de classe: {e}")
                    else:
                        st.warning("Não há dados suficientes para análise por classe.")
                
                with col_grafico2:
                    st.subheader("⛽ Consumo por Tipo de Combustível")
                    if not df_consumo_combustivel.empty and 'tipo_combustivel' in df_consumo_combustivel.columns:
                        try:
                            consumo_por_combustivel = df_consumo_combustivel.groupby("tipo_combustivel")["Qtde_Litros"].sum().sort_values(ascending=False).reset_index()
                            
                            # Criar gráfico de pizza
                            fig_pizza_combustivel = px.pie(
                                consumo_por_combustivel, 
                                values='Qtde_Litros', 
                                names='tipo_combustivel',
                                title="Proporção de Consumo por Combustível",
                                hole=0.3
                            )
                            fig_pizza_combustivel.update_traces(textposition='inside', textinfo='percent+label')
                            fig_pizza_combustivel.update_layout(height=400)
                            st.plotly_chart(fig_pizza_combustivel, use_container_width=True)
                            
                            # Mostrar totais
                            st.info(f"**Total de tipos de combustível:** {len(consumo_por_combustivel)}")
                            st.info(f"**Total de litros consumidos:** {formatar_brasileiro_int(consumo_por_combustivel['Qtde_Litros'].sum())} L")
                        except Exception:
                            df_consumo_combustivel['tipo_combustivel'] = 'Diesel S500'
                            consumo_por_combustivel = df_consumo_combustivel.groupby("tipo_combustivel")["Qtde_Litros"].sum().reset_index()
                            st.info(f"**Total de litros consumidos:** {formatar_brasileiro_int(consumo_por_combustivel['Qtde_Litros'].sum())} L")
                    else:
                        st.warning("Não há dados suficientes para análise por combustível.")
                        

                    st.markdown("---")
                    st.subheader("📊 Demonstrativos Detalhados dos Pneus")

                    df_pneus_all = get_pneus_historico()
                    if not df_pneus_all.empty:
                        # Adicione colunas de status e vida se não existirem
                        if 'status' not in df_pneus_all.columns:
                            df_pneus_all['status'] = 'Ativo'
                        if 'vida_atual' not in df_pneus_all.columns:
                            df_pneus_all['vida_atual'] = 1

                        total_pneus = len(df_pneus_all)
                        ativos = df_pneus_all[df_pneus_all['status'].str.lower() == 'ativo'].shape[0]
                        sucateados = df_pneus_all[df_pneus_all['status'].str.lower() == 'sucateado'].shape[0]
                        reformados = df_pneus_all[df_pneus_all['status'].str.lower() == 'reformado'].shape[0]
                        vidas = df_pneus_all['vida_atual'].value_counts().sort_index()
                        marcas = df_pneus_all['marca'].value_counts()
                        modelos = df_pneus_all['modelo'].value_counts()
                        posicoes = df_pneus_all['posicao'].value_counts()

                        col1, col2, col3, col4 = st.columns(4)
                        col1.metric("Total de Pneus", total_pneus)
                        col2.metric("Ativos", ativos)
                        col3.metric("Sucateados", sucateados)
                        col4.metric("Reformados", reformados)

                        st.markdown("#### Distribuição por Vida Atual")
                        vidas_df = vidas.reset_index()
                        vidas_df.columns = ["Vida", "Quantidade"]
                        st.dataframe(vidas_df)

                        st.markdown("#### Distribuição por Status")
                        status_df = df_pneus_all['status'].value_counts().reset_index()
                        status_df.columns = ["Status", "Quantidade"]
                        st.dataframe(status_df)

                        st.markdown("#### Distribuição por Marca")
                        st.dataframe(marcas.reset_index().rename(columns={'index': 'Marca', 'marca': 'Quantidade'}))

                        st.markdown("#### Distribuição por Modelo")
                        st.dataframe(modelos.reset_index().rename(columns={'index': 'Modelo', 'modelo': 'Quantidade'}))

                        st.markdown("#### Distribuição por Posição")
                        st.dataframe(posicoes.reset_index().rename(columns={'index': 'Posição', 'posicao': 'Quantidade'}))

                        # Gráficos
                        fig_status = px.pie(status_df, names='Status', values='Quantidade', title='Status dos Pneus')
                        st.plotly_chart(fig_status, use_container_width=True)

                        fig_vidas = px.bar(vidas_df, x='Vida', y='Quantidade', title='Quantidade de Pneus por Vida')
                        st.plotly_chart(fig_vidas, use_container_width=True)

                        fig_marcas = px.bar(marcas.reset_index(), x='index', y='marca', title='Quantidade por Marca')
                        st.plotly_chart(fig_marcas, use_container_width=True)

                        fig_modelos = px.bar(modelos.reset_index(), x='index', y='modelo', title='Quantidade por Modelo')
                        st.plotly_chart(fig_modelos, use_container_width=True)

                        fig_posicoes = px.bar(posicoes.reset_index(), x='index', y='posicao', title='Quantidade por Posição')
                        st.plotly_chart(fig_posicoes, use_container_width=True)

                    else:
                        st.info("Nenhum pneu cadastrado para demonstrativo.")

                    st.markdown("---")
                    st.subheader("🛢️ Demonstrativos de Lubrificantes")

                    ensure_lubrificantes_schema()
                    conn = sqlite3.connect(DB_PATH)
                    df_lub = pd.read_sql("SELECT * FROM lubrificantes", conn)
                    df_mov = pd.read_sql("SELECT * FROM lubrificantes_movimentacoes", conn)

                    st.write("**Estoque Atual de Lubrificantes:**")
                    if not df_lub.empty:
                            # Separar por tipo
                            df_oleos = df_lub[df_lub['tipo'].str.lower() == 'óleo']
                            df_graxas = df_lub[df_lub['tipo'].str.lower() == 'graxa']

                            col_o, col_g = st.columns(2)
                            with col_o:
                                st.markdown("#### Estoque de Óleos")
                                if not df_oleos.empty:
                                    fig_oleos = px.bar(
                                        df_oleos,
                                        x='nome',
                                        y='quantidade_estoque',
                                        color='viscosidade',
                                        text='quantidade_estoque',
                                        title="Óleos - Estoque Atual",
                                        labels={'quantidade_estoque': 'Qtd. Estoque', 'nome': 'Óleo'}
                                    )
                                    st.plotly_chart(fig_oleos, use_container_width=True)
                                else:
                                    st.info("Nenhum óleo cadastrado.")

                            with col_g:
                                st.markdown("#### Estoque de Graxas")
                                if not df_graxas.empty:
                                    fig_graxas = px.bar(
                                        df_graxas,
                                        x='nome',
                                        y='quantidade_estoque',
                                        color='viscosidade',
                                        text='quantidade_estoque',
                                        title="Graxas - Estoque Atual",
                                        labels={'quantidade_estoque': 'Qtd. Estoque', 'nome': 'Graxa'}
                                    )
                                    st.plotly_chart(fig_graxas, use_container_width=True)
                                else:
                                    st.info("Nenhuma graxa cadastrada.")

                            # Pizza geral
                            df_lub['tipo'] = df_lub['tipo'].fillna('óleo')
                            fig_pizza = px.pie(
                                df_lub,
                                names='tipo',
                                values='quantidade_estoque',
                                title="Proporção de Estoque: Óleos vs Graxas"
                            )
                            st.plotly_chart(fig_pizza, use_container_width=True)

                            st.write("**Movimentações Recentes:**")
                            df_mov['data'] = pd.to_datetime(df_mov['data'], errors='coerce')
                            df_mov = df_mov.sort_values('data', ascending=False)
                            st.dataframe(df_mov.head(20))
                    else:
                            st.info("Nenhum lubrificante cadastrado.")

                    conn.close()
            
import streamlit as st
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime, date, timedelta
import os
import plotly.express as px
import hashlib
import json
import base64
import io

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
            df_comp_historico = pd.read_sql_query("SELECT rowid, * FROM componentes_historico", conn)
            df_checklist_regras = pd.read_sql_query("SELECT * FROM checklist_regras", conn)
            df_checklist_itens = pd.read_sql_query("SELECT * FROM checklist_itens", conn)
            df_checklist_historico = pd.read_sql_query("SELECT rowid, * FROM checklist_historico", conn)

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

        # Vincula informações de motorista aos abastecimentos (merge durante o load)
        try:
            with sqlite3.connect(db_path, check_same_thread=False) as conn:
                df_motoristas = pd.read_sql_query("SELECT codigo_pessoa, matricula, nome FROM motoristas", conn)
            if not df_motoristas.empty:
                df_merged = df_merged.merge(
                    df_motoristas.rename(columns={"codigo_pessoa": "Cod_Pessoa", "matricula": "Matricula", "nome": "Nome_Motorista"}),
                    on=["Cod_Pessoa", "Matricula"], how="left"
                )
        except Exception:
            pass
        
        # Garante que a classe operacional em df_frotas está atualizada
        classe_map = df_merged.dropna(subset=['Classe_Operacional']).groupby('Cod_Equip')['Classe_Operacional'].first()
        df_frotas['Classe_Operacional'] = df_frotas['Cod_Equip'].map(classe_map).fillna(df_frotas.get('Classe_Operacional'))

        # Adiciona coluna de tipo de combustível se não existir
        if 'tipo_combustivel' not in df_frotas.columns:
            df_frotas['tipo_combustivel'] = 'Diesel S500'  # Valor padrão
        
        # Garantir que a coluna existe e tem valores válidos
        if 'tipo_combustivel' in df_frotas.columns:
            # Preencher valores nulos com padrão
            df_frotas['tipo_combustivel'] = df_frotas['tipo_combustivel'].fillna('Diesel S500')
        else:
            df_frotas['tipo_combustivel'] = 'Diesel S500'

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
                Safra, "Mês", "Classe Operacional", Matricula, Cod_Pessoa
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        valores = (
            dados['cod_equip'],
            dados['data'],
            dados['qtde_litros'],
            dados['hod_hor_atual'],
            dados['safra'],
            dados['mes'],
            dados['classe_operacional'],
            dados.get('matricula'),
            dados.get('cod_pessoa')
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


def excluir_manutencao_componente(db_path: str, cod_equip: int, nome_componente: str, data: str, hod_hor: float) -> bool:
    """Exclui um registro de manutenção de componente do banco de dados usando uma combinação única de campos."""
    try:
        conn = sqlite3.connect(db_path, check_same_thread=False)
        cursor = conn.cursor()
        
        # Converter tipos de dados para garantir compatibilidade
        cod_equip = int(cod_equip)
        nome_componente = str(nome_componente)
        data = str(data)
        hod_hor = float(hod_hor)
        
        # Debug: verificar todos os registros na tabela
        cursor.execute("SELECT rowid, Cod_Equip, nome_componente, Data, Hod_Hor_No_Servico FROM componentes_historico")
        all_records = cursor.fetchall()
        
        # Debug: verificar se há registros com valores similares
        cursor.execute(
            "SELECT rowid, Cod_Equip, nome_componente, Data, Hod_Hor_No_Servico FROM componentes_historico WHERE Cod_Equip = ?", 
            (cod_equip,)
        )
        similar_records = cursor.fetchall()
        
        # Primeiro, vamos verificar se o registro existe
        cursor.execute(
            "SELECT COUNT(*) FROM componentes_historico WHERE Cod_Equip = ? AND nome_componente = ? AND Data = ? AND Hod_Hor_No_Servico = ?", 
            (cod_equip, nome_componente, data, hod_hor)
        )
        count = cursor.fetchone()[0]
        
        if count == 0:
            # Debug: retornar informações sobre o que foi encontrado
            debug_info = f"""
            Registro não encontrado para exclusão.
            
            Valores procurados (após conversão):
            - Cod_Equip: {cod_equip} (tipo: {type(cod_equip)})
            - Nome Componente: {nome_componente} (tipo: {type(nome_componente)})
            - Data: {data} (tipo: {type(data)})
            - Hod_Hor: {hod_hor} (tipo: {type(hod_hor)})
            
            Registros similares encontrados (mesmo Cod_Equip):
            {similar_records}
            
            Todos os registros na tabela:
            {all_records}
            """
            st.error(debug_info)
            return False
        
        # Agora vamos excluir
        cursor.execute(
            "DELETE FROM componentes_historico WHERE Cod_Equip = ? AND nome_componente = ? AND Data = ? AND Hod_Hor_No_Servico = ?", 
            (cod_equip, nome_componente, data, hod_hor)
        )
        
        # Forçar commit imediato
        conn.commit()
        
        # Verificar se foi realmente excluído
        rows_deleted = cursor.rowcount
        if rows_deleted > 0:
            # Verificar novamente se o registro foi realmente excluído
            cursor.execute(
                "SELECT COUNT(*) FROM componentes_historico WHERE Cod_Equip = ? AND nome_componente = ? AND Data = ? AND Hod_Hor_No_Servico = ?", 
                (cod_equip, nome_componente, data, hod_hor)
            )
            count_after = cursor.fetchone()[0]
            
            if count_after == 0:
                # Forçar sincronização do banco
                cursor.execute("PRAGMA wal_checkpoint(FULL)")
                cursor.execute("PRAGMA synchronous=FULL")
                conn.commit()
                
                # Salvar backup automático para persistência no Streamlit Cloud
                backup_success, backup_msg = save_backup_to_session_state()
                if backup_success:
                    st.success(f"Manutenção de componente excluída com sucesso! ({rows_deleted} registro(s) removido(s)) | Backup salvo: {backup_msg}")
                else:
                    st.success(f"Manutenção de componente excluída com sucesso! ({rows_deleted} registro(s) removido(s)) | Aviso: {backup_msg}")
                
                conn.close()
                return True
            else:
                st.error("Erro: Registro ainda existe após exclusão")
                conn.close()
                return False
        else:
            st.error("Nenhum registro foi excluído")
            conn.close()
            return False
            
    except Exception as e:
        st.error(f"Erro ao excluir manutenção de componente do banco de dados: {e}")
        return False
    finally:
        if 'conn' in locals():
            conn.close()


def excluir_manutencao(db_path: str, rowid: int) -> bool:
    """Exclui um registro de manutenção do banco de dados usando seu rowid."""
    try:
        conn = sqlite3.connect(db_path, check_same_thread=False)
        cursor = conn.cursor()
        sql = "DELETE FROM manutencoes WHERE rowid = ?"
        cursor.execute(sql, (rowid,))
        conn.commit()
        conn.close()
        return True
    except sqlite3.Error as e:
        st.error(f"Erro ao excluir manutenção do banco de dados: {e}")
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
                "Classe Operacional", ATIVO, tipo_combustivel
            ) VALUES (?, ?, ?, ?, ?, ?)
        """
        valores = (
            dados['cod_equip'],
            dados['descricao'],
            dados['placa'],
            dados['classe_op'],
            dados['ativo'],
            dados.get('tipo_combustivel', 'Diesel S500')
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
                "Cód. Equip." = ?, Data = ?, "Qtde Litros" = ?, Hod_Hor_Atual = ?, Safra = ?, Matricula = ?, Cod_Pessoa = ?
            WHERE rowid = ?
        """
        valores = (
            dados['cod_equip'], dados['data'], dados['qtde_litros'], dados['hod_hor_atual'], dados['safra'],
            dados.get('matricula'), dados.get('cod_pessoa'), rowid
        )
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


def editar_manutencao_componente(db_path: str, rowid: int, dados: dict) -> bool:
    """Edita um registro de manutenção de componente existente."""
    try:
        conn = sqlite3.connect(db_path, check_same_thread=False)
        cursor = conn.cursor()
        sql = """
            UPDATE componentes_historico 
            SET Cod_Equip = ?, nome_componente = ?, Observacoes = ?, Data = ?, Hod_Hor_No_Servico = ?
            WHERE rowid = ?
        """
        valores = (
            dados['cod_equip'],
            dados['componente'],
            dados['acao'],
            dados['data'],
            dados['hod_hor_servico'],
            rowid
        )
        cursor.execute(sql, valores)
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        st.error(f"Erro ao editar manutenção de componente no banco de dados: {e}")
        return False

def importar_abastecimentos_de_planilha(db_path: str, arquivo_carregado) -> tuple[int, int, str]:
    """Lê uma planilha, verifica por duplicados, e insere os novos dados. Aceita opcionalmente as colunas Matricula e Cod_Pessoa."""
    try:
        df_novo = pd.read_excel(arquivo_carregado)
        
        mapa_colunas = {
            "Cód. Equip.": "Cód. Equip.",
            "Data": "Data",
            "Qtde Litros": "Qtde Litros",
            "Hod. Hor. Atual": "Hod_Hor_Atual",
            "Safra": "Safra",
            "Mês": "Mês",
            "Classe Operacional": "Classe Operacional",
            "Matricula": "Matricula",
            "Cod_Pessoa": "Cod_Pessoa",
        }
        df_novo = df_novo.rename(columns={k: v for k, v in mapa_colunas.items() if k in df_novo.columns})

        colunas_necessarias = ["Cód. Equip.", "Data", "Qtde Litros", "Hod_Hor_Atual", "Safra", "Mês", "Classe Operacional"]
        colunas_opcionais = ["Matricula", "Cod_Pessoa"]
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

        colunas_insert = colunas_necessarias + [c for c in colunas_opcionais if c in df_para_inserir.columns]
        df_para_inserir_final = df_para_inserir[colunas_insert]
        registros = [tuple(x) for x in df_para_inserir_final.to_numpy()]
        
        cursor = conn.cursor()
        placeholders = ", ".join(["?"] * len(colunas_insert))
        sql = f"INSERT INTO abastecimentos ({', '.join(f'\"{col}\"' for col in colunas_insert)}) VALUES ({placeholders})"
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
                DESCRICAO_EQUIPAMENTO = ?, PLACA = ?, "Classe Operacional" = ?, ATIVO = ?, tipo_combustivel = ?
            WHERE COD_EQUIPAMENTO = ?
        """
        valores = (dados['descricao'], dados['placa'], dados['classe_op'], dados['ativo'], dados.get('tipo_combustivel', 'Diesel S500'), cod_equip)
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

def add_component_rule_advanced(classe, componente, intervalo, lubrificante_id=None, tipo_manutencao="Troca", capacidade_litros=0.0):
    """Adiciona uma nova regra de componente com informações de lubrificante e tipo de manutenção."""
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            
            # Verificar se a tabela tem as colunas necessárias
            cursor.execute("PRAGMA table_info(componentes_regras)")
            columns = [column[1] for column in cursor.fetchall()]
            
            # Adicionar colunas se não existirem
            if 'lubrificante_id' not in columns:
                cursor.execute("ALTER TABLE componentes_regras ADD COLUMN lubrificante_id INTEGER")
            if 'tipo_manutencao' not in columns:
                cursor.execute("ALTER TABLE componentes_regras ADD COLUMN tipo_manutencao TEXT DEFAULT 'Troca'")
            
            cursor.execute(
                "INSERT INTO componentes_regras (classe_operacional, nome_componente, intervalo_padrao, lubrificante_id, tipo_manutencao, capacidade_litros) VALUES (?, ?, ?, ?, ?, ?)",
                (classe, componente, intervalo, lubrificante_id, tipo_manutencao, capacidade_litros)
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

def add_component_service_advanced(cod_equip, componente, data, hod_hor, tipo_servico, lubrificante_utilizado=None, obs=""):
    """Adiciona um novo registo de serviço de componente com informações detalhadas."""
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            
            # Verificar se a tabela tem as colunas necessárias
            cursor.execute("PRAGMA table_info(componentes_historico)")
            columns = [column[1] for column in cursor.fetchall()]
            
            # Adicionar colunas se não existirem
            if 'tipo_servico' not in columns:
                cursor.execute("ALTER TABLE componentes_historico ADD COLUMN tipo_servico TEXT DEFAULT 'Troca'")
            if 'lubrificante_utilizado' not in columns:
                cursor.execute("ALTER TABLE componentes_historico ADD COLUMN lubrificante_utilizado TEXT")
            
            cursor.execute(
                "INSERT INTO componentes_historico (Cod_Equip, nome_componente, Data, Hod_Hor_No_Servico, tipo_servico, lubrificante_utilizado, Observacoes) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (cod_equip, componente, data, hod_hor, tipo_servico, lubrificante_utilizado, obs)
            )
            conn.commit()
        return True, "Serviço de componente registado com sucesso."
    except Exception as e:
        return False, f"Erro ao registar serviço: {e}"

def get_component_status(cod_equip, componente):
    """Obtém o status atual de um componente específico de um equipamento."""
    try:
        with sqlite3.connect(DB_PATH) as conn:
            # Buscar a última manutenção do componente
            query = """
            SELECT Data, Hod_Hor_No_Servico, tipo_servico, lubrificante_utilizado, Observacoes
            FROM componentes_historico 
            WHERE Cod_Equip = ? AND nome_componente = ?
            ORDER BY Data DESC, Hod_Hor_No_Servico DESC
            LIMIT 1
            """
            df_ultima = pd.read_sql_query(query, conn, params=(cod_equip, componente))
            
            # Buscar a regra do componente para obter o intervalo
            query_regra = """
            SELECT intervalo_padrao, lubrificante_id, tipo_manutencao
            FROM componentes_regras cr
            JOIN frotas f ON cr.classe_operacional = f."Classe Operacional"
            WHERE f.COD_EQUIPAMENTO = ? AND cr.nome_componente = ?
            """
            df_regra = pd.read_sql_query(query_regra, conn, params=(cod_equip, componente))
            
            # Buscar o hodômetro/horímetro atual do equipamento
            query_hod = """
            SELECT Hod_Hor_Atual FROM abastecimentos 
            WHERE Cod_Equip = ? 
            ORDER BY Data DESC, Hod_Hor_Atual DESC 
            LIMIT 1
            """
            df_hod = pd.read_sql_query(query_hod, conn, params=(cod_equip,))
            
            return df_ultima, df_regra, df_hod
            
    except Exception as e:
        st.error(f"Erro ao obter status do componente: {e}")
        return None, None, None

def get_component_maintenance_count(cod_equip, componente):
    """Obtém o número total de manutenções realizadas em um componente."""
    try:
        with sqlite3.connect(DB_PATH) as conn:
            query = """
            SELECT COUNT(*) as total_manutencoes,
                   COUNT(CASE WHEN tipo_servico = 'Troca' THEN 1 END) as total_trocas,
                   COUNT(CASE WHEN tipo_servico = 'Remonta' THEN 1 END) as total_remontas
            FROM componentes_historico 
            WHERE Cod_Equip = ? AND nome_componente = ?
            """
            df_count = pd.read_sql_query(query, conn, params=(cod_equip, componente))
            return df_count.iloc[0] if not df_count.empty else {'total_manutencoes': 0, 'total_trocas': 0, 'total_remontas': 0}
            
    except Exception as e:
        st.error(f"Erro ao obter contagem de manutenções: {e}")
        return {'total_manutencoes': 0, 'total_trocas': 0, 'total_remontas': 0}

def editar_manutencao_componente_advanced(DB_PATH, rowid, dados_editados):
    """Edita uma manutenção de componente com informações avançadas."""
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            
            # Verificar se a tabela tem as colunas necessárias
            cursor.execute("PRAGMA table_info(componentes_historico)")
            columns = [column[1] for column in cursor.fetchall()]
            
            # Adicionar colunas se não existirem
            if 'tipo_servico' not in columns:
                cursor.execute("ALTER TABLE componentes_historico ADD COLUMN tipo_servico TEXT DEFAULT 'Troca'")
            if 'lubrificante_utilizado' not in columns:
                cursor.execute("ALTER TABLE componentes_historico ADD COLUMN lubrificante_utilizado TEXT")
            
            # Atualizar os dados
            cursor.execute("""
                UPDATE componentes_historico 
                SET Cod_Equip = ?, nome_componente = ?, Data = ?, Hod_Hor_No_Servico = ?, 
                    Observacoes = ?, tipo_servico = ?, lubrificante_utilizado = ?
                WHERE rowid = ?
            """, (
                dados_editados['cod_equip'],
                dados_editados['componente'],
                dados_editados['data'],
                dados_editados['hod_hor_servico'],
                dados_editados['acao'],
                dados_editados['tipo_servico'],
                dados_editados['lubrificante_utilizado'],
                rowid
            ))
            conn.commit()
        return True, "Manutenção de componente atualizada com sucesso."
    except Exception as e:
        return False, f"Erro ao atualizar manutenção de componente: {e}"

def update_component_rule(rule_id, nome_componente, intervalo, lubrificante_id=None, tipo_manutencao="Troca"):
    """Atualiza uma regra de componente existente."""
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            
            # Verificar se a tabela tem as colunas necessárias
            cursor.execute("PRAGMA table_info(componentes_regras)")
            columns = [column[1] for column in cursor.fetchall()]
            
            # Adicionar colunas se não existirem
            if 'lubrificante_id' not in columns:
                cursor.execute("ALTER TABLE componentes_regras ADD COLUMN lubrificante_id INTEGER")
            if 'tipo_manutencao' not in columns:
                cursor.execute("ALTER TABLE componentes_regras ADD COLUMN tipo_manutencao TEXT DEFAULT 'Troca'")
            
            # Atualizar os dados
            cursor.execute("""
                UPDATE componentes_regras 
                SET nome_componente = ?, intervalo_padrao = ?, lubrificante_id = ?, tipo_manutencao = ?
                WHERE id_regra = ?
            """, (nome_componente, intervalo, lubrificante_id, tipo_manutencao, rule_id))
            conn.commit()
        return True, f"Componente '{nome_componente}' atualizado com sucesso."
    except Exception as e:
        return False, f"Erro ao atualizar componente: {e}"


def get_frota_combustivel(cod_equip):
    """Obtém o tipo de combustível de uma frota específica."""
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT tipo_combustivel FROM frotas WHERE COD_EQUIPAMENTO = ?", (cod_equip,))
            result = cursor.fetchone()
            return result[0] if result else None
    except Exception as e:
        st.error(f"Erro ao obter tipo de combustível: {e}")
        return None


def update_frota_combustivel(cod_equip, tipo_combustivel):
    """Atualiza o tipo de combustível de uma frota específica."""
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE frotas SET tipo_combustivel = ? WHERE COD_EQUIPAMENTO = ?", (tipo_combustivel, cod_equip))
            conn.commit()
        return True, f"Tipo de combustível atualizado para {tipo_combustivel}"
    except Exception as e:
        return False, f"Erro ao atualizar tipo de combustível: {e}"


def update_classe_combustivel(classe_operacional, tipo_combustivel):
    """Atualiza o tipo de combustível de todas as frotas de uma classe."""
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE frotas SET tipo_combustivel = ? WHERE \"Classe Operacional\" = ?", (tipo_combustivel, classe_operacional))
            rows_updated = cursor.rowcount
            conn.commit()
        return True, f"Tipo de combustível atualizado para {tipo_combustivel} em {rows_updated} frotas da classe {classe_operacional}"
    except Exception as e:
        return False, f"Erro ao atualizar tipo de combustível da classe: {e}"


def add_tipo_combustivel_column():
    """Adiciona a coluna tipo_combustivel à tabela frotas se ela não existir."""
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            # Verificar se a coluna existe
            cursor.execute("PRAGMA table_info(frotas)")
            columns = [column[1] for column in cursor.fetchall()]
            
            if 'tipo_combustivel' not in columns:
                cursor.execute("ALTER TABLE frotas ADD COLUMN tipo_combustivel TEXT DEFAULT 'Diesel S500'")
                conn.commit()
                return True, "Coluna tipo_combustivel adicionada com sucesso"
            else:
                return True, "Coluna tipo_combustivel já existe"
    except Exception as e:
        return False, f"Erro ao adicionar coluna tipo_combustivel: {e}"


def ensure_motoristas_schema():
    """Garante a existência da tabela de motoristas e das colunas de vínculo em abastecimentos."""
    try:
        with sqlite3.connect(DB_PATH, check_same_thread=False) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS motoristas (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    codigo_pessoa TEXT,
                    matricula TEXT UNIQUE,
                    nome TEXT,
                    ativo TEXT DEFAULT 'ATIVO'
                )
                """
            )
            cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_motoristas_matricula ON motoristas(matricula)")
            cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_motoristas_codigo_pessoa ON motoristas(codigo_pessoa)")

            cursor.execute("PRAGMA table_info(abastecimentos)")
            cols = [c[1] for c in cursor.fetchall()]
            if 'Matricula' not in cols:
                cursor.execute("ALTER TABLE abastecimentos ADD COLUMN Matricula TEXT")
            if 'Cod_Pessoa' not in cols:
                cursor.execute("ALTER TABLE abastecimentos ADD COLUMN Cod_Pessoa TEXT")
            conn.commit()
        return True, "Esquema de motoristas verificado"
    except Exception as e:
        return False, f"Erro ao verificar esquema de motoristas: {e}"


def get_all_motoristas() -> pd.DataFrame:
    """Retorna o DataFrame de motoristas."""
    try:
        with sqlite3.connect(DB_PATH, check_same_thread=False) as conn:
            return pd.read_sql_query("SELECT * FROM motoristas", conn)
    except Exception:
        return pd.DataFrame(columns=['id', 'codigo_pessoa', 'matricula', 'nome', 'ativo'])


def importar_motoristas_de_planilha(db_path: str, arquivo_carregado):
    """Importa motoristas a partir de planilha Excel. Espera colunas: Matricula, Nome e opcional Cod_Pessoa/Código Pessoa."""
    try:
        df_mot = pd.read_excel(arquivo_carregado)
        df_mot.columns = [c.strip() for c in df_mot.columns]
        renomeios = {
            'Matrícula': 'Matricula', 'matricula': 'Matricula', 'MATRICULA': 'Matricula',
            'Nome': 'Nome', 'nome': 'Nome', 'NOME': 'Nome',
            'Cod_Pessoa': 'Cod_Pessoa', 'Código Pessoa': 'Cod_Pessoa', 'codigo_pessoa': 'Cod_Pessoa', 'CODIGO_PESSOA': 'Cod_Pessoa'
        }
        df_mot.rename(columns={k: v for k, v in renomeios.items() if k in df_mot.columns}, inplace=True)
        obrig = ['Matricula', 'Nome']
        faltando = [c for c in obrig if c not in df_mot.columns]
        if faltando:
            return 0, 0, f"Erro: Colunas obrigatórias não encontradas: {', '.join(faltando)}"
        if 'Cod_Pessoa' not in df_mot.columns:
            df_mot['Cod_Pessoa'] = None
        df_mot = df_mot.dropna(subset=['Matricula', 'Nome']).copy()
        df_mot['Matricula'] = df_mot['Matricula'].astype(str).str.strip()
        df_mot['Nome'] = df_mot['Nome'].astype(str).str.strip()
        df_mot['Cod_Pessoa'] = df_mot['Cod_Pessoa'].astype(str).str.strip()
        df_mot = df_mot.drop_duplicates(subset=['Matricula'])
        with sqlite3.connect(db_path, check_same_thread=False) as conn:
            existentes = pd.read_sql_query("SELECT matricula FROM motoristas", conn)
            set_exist = set(existentes['matricula'].astype(str)) if not existentes.empty else set()
            df_novos = df_mot[~df_mot['Matricula'].isin(set_exist)].copy()
            if df_novos.empty:
                return 0, len(df_mot), "Nenhum motorista novo para importar. Todos já existem."
            registros = [
                (row.get('Cod_Pessoa', None), row['Matricula'], row['Nome'], 'ATIVO')
                for _, row in df_novos.iterrows()
            ]
            cur = conn.cursor()
            cur.executemany(
                "INSERT INTO motoristas (codigo_pessoa, matricula, nome, ativo) VALUES (?, ?, ?, ?)",
                registros
            )
            conn.commit()
            inseridos = cur.rowcount if cur.rowcount is not None else len(registros)
            duplicados = len(df_mot) - len(df_novos)
            return inseridos, duplicados, f"{inseridos} motoristas importados com sucesso. {duplicados} já existiam."
    except Exception as e:
        return 0, 0, f"Ocorreu um erro inesperado durante a importação de motoristas: {e}"
    
def ensure_pneus_schema():
    """Garante a existência da tabela de histórico de pneus."""
    try:
        with sqlite3.connect(DB_PATH, check_same_thread=False) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS pneus_historico (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    Cod_Equip INTEGER,
                    posicao TEXT,
                    marca TEXT,
                    modelo TEXT,
                    data_instalacao TEXT,
                    hodometro_instalacao REAL,
                    vida_util_km REAL,
                    observacoes TEXT,
                    status TEXT DEFAULT 'Ativo',
                    vida_atual INTEGER DEFAULT 1
                )
            """)
            # Adiciona colunas se não existirem
            cursor.execute("PRAGMA table_info(pneus_historico)")
            cols = [c[1] for c in cursor.fetchall()]
            if 'status' not in cols:
                cursor.execute("ALTER TABLE pneus_historico ADD COLUMN status TEXT DEFAULT 'Ativo'")
            if 'vida_atual' not in cols:
                cursor.execute("ALTER TABLE pneus_historico ADD COLUMN vida_atual INTEGER DEFAULT 1")
            conn.commit()
        return True, "Tabela de pneus verificada"
    except Exception as e:
        return False, f"Erro ao criar tabela de pneus: {e}"

def importar_pneus_de_planilha(db_path: str, arquivo_carregado):
    """Importa histórico de pneus de uma planilha Excel, verificando duplicatas."""
    try:
        df_pneus = pd.read_excel(arquivo_carregado)
        df_pneus.columns = [c.strip() for c in df_pneus.columns]
        obrig = ['Cod_Equip', 'posicao', 'marca', 'modelo', 'data_instalacao', 'hodometro_instalacao', 'vida_util_km']
        faltando = [c for c in obrig if c not in df_pneus.columns]
        if faltando:
            return 0, 0, f"Colunas obrigatórias faltando: {', '.join(faltando)}"
        
        if 'observacoes' not in df_pneus.columns:
            df_pneus['observacoes'] = ""
        
        # Limpar dados e remover linhas com valores nulos obrigatórios
        df_pneus = df_pneus.dropna(subset=['Cod_Equip', 'posicao'])
        
        # Normalizar tipos de dados
        df_pneus['Cod_Equip'] = df_pneus['Cod_Equip'].astype(str)
        df_pneus['posicao'] = df_pneus['posicao'].astype(str).str.strip()
        df_pneus['data_instalacao'] = pd.to_datetime(df_pneus['data_instalacao']).dt.strftime('%Y-%m-%d')
        
        # Remover duplicatas na própria planilha baseada em chave única
        df_pneus = df_pneus.drop_duplicates(subset=['Cod_Equip', 'posicao', 'data_instalacao', 'hodometro_instalacao'])
        
        with sqlite3.connect(db_path, check_same_thread=False) as conn:
            # Buscar registros existentes para verificar duplicatas
            df_existente = pd.read_sql_query("SELECT Cod_Equip, posicao, data_instalacao, hodometro_instalacao FROM pneus_historico", conn)
            
            if not df_existente.empty:
                # Normalizar dados existentes para comparação
                df_existente['Cod_Equip'] = df_existente['Cod_Equip'].astype(str)
                df_existente['posicao'] = df_existente['posicao'].astype(str).str.strip()
                df_existente['data_instalacao'] = pd.to_datetime(df_existente['data_instalacao']).dt.strftime('%Y-%m-%d')
                
                # Criar chaves únicas para comparação
                df_pneus['chave_unica'] = (df_pneus['Cod_Equip'] + '_' + 
                                          df_pneus['posicao'] + '_' + 
                                          df_pneus['data_instalacao'] + '_' + 
                                          df_pneus['hodometro_instalacao'].astype(str))
                
                df_existente['chave_unica'] = (df_existente['Cod_Equip'] + '_' + 
                                              df_existente['posicao'] + '_' + 
                                              df_existente['data_instalacao'] + '_' + 
                                              df_existente['hodometro_instalacao'].astype(str))
                
                # Filtrar apenas registros que não existem
                df_para_inserir = df_pneus[~df_pneus['chave_unica'].isin(df_existente['chave_unica'])]
            else:
                df_para_inserir = df_pneus
            
            num_duplicados = len(df_pneus) - len(df_para_inserir)
            
            if df_para_inserir.empty:
                return 0, num_duplicados, "Nenhum pneu novo para importar. Todos os registros da planilha já existem na base de dados."
            
            # Preparar registros para inserção
            colunas_insert = obrig + ['observacoes']
            df_para_inserir_final = df_para_inserir[colunas_insert]
            registros = [tuple(x) for x in df_para_inserir_final.fillna('').to_numpy()]
            
            cur = conn.cursor()
            placeholders = ", ".join(["?"] * len(colunas_insert))
            sql = f"INSERT INTO pneus_historico ({', '.join(f'\"{col}\"' for col in colunas_insert)}) VALUES ({placeholders})"
            cur.executemany(sql, registros)
            conn.commit()
            
            num_inseridos = len(registros)
            
            mensagem_sucesso = f"{num_inseridos} pneus novos foram importados com sucesso."
            if num_duplicados > 0:
                mensagem_sucesso += f" {num_duplicados} registros duplicados foram ignorados."
            
            return num_inseridos, num_duplicados, mensagem_sucesso
            
    except Exception as e:
        return 0, 0, f"Erro ao importar pneus: {e}"

def get_pneus_historico(cod_equip=None):
    """Retorna o histórico de pneus, opcionalmente filtrando por frota."""
    try:
        with sqlite3.connect(DB_PATH, check_same_thread=False) as conn:
            query = "SELECT * FROM pneus_historico"
            params = ()
            if cod_equip:
                query += " WHERE Cod_Equip = ?"
                params = (cod_equip,)
            return pd.read_sql_query(query, conn, params=params)
    except Exception:
        return pd.DataFrame()

def ensure_precos_combustivel_schema():
    """Garante a existência da tabela de preços por tipo de combustível."""
    try:
        with sqlite3.connect(DB_PATH, check_same_thread=False) as conn:
            cur = conn.cursor()
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS precos_combustivel (
                    tipo_combustivel TEXT PRIMARY KEY,
                    preco REAL
                )
                """
            )
            tipos = ['Diesel S500', 'Diesel S10', 'Gasolina', 'Etanol', 'Biodiesel']
            for t in tipos:
                cur.execute("INSERT OR IGNORE INTO precos_combustivel (tipo_combustivel, preco) VALUES (?, ?)", (t, NULL))
            conn.commit()
        return True, "Tabela de preços verificada"
    except Exception as e:
        return False, f"Erro ao verificar tabela de preços: {e}"


def get_precos_combustivel_map() -> dict:
    """Retorna um dicionário {tipo_combustivel: preco}."""
    try:
        with sqlite3.connect(DB_PATH, check_same_thread=False) as conn:
            dfp = pd.read_sql_query("SELECT tipo_combustivel, preco FROM precos_combustivel", conn)
        return {row['tipo_combustivel']: row['preco'] for _, row in dfp.iterrows()}
    except Exception:
        return {}


def upsert_preco_combustivel(tipo: str, preco: float) -> tuple[bool, str]:
    """Cria/atualiza preço para um tipo de combustível."""
    try:
        with sqlite3.connect(DB_PATH, check_same_thread=False) as conn:
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO precos_combustivel (tipo_combustivel, preco) VALUES (?, ?) ON CONFLICT(tipo_combustivel) DO UPDATE SET preco=excluded.preco",
                (tipo, preco)
            )
            conn.commit()
        return True, f"Preço atualizado para {tipo}"
    except Exception as e:
        return False, f"Erro ao atualizar preço: {e}"
    
def ensure_lubrificantes_schema():
    """Garante a existência da tabela de lubrificantes, movimentações e almoxarifados."""
    try:
        with sqlite3.connect(DB_PATH, check_same_thread=False) as conn:
            cursor = conn.cursor()
            
            # Tabela de lubrificantes
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS lubrificantes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    nome TEXT,
                    viscosidade TEXT,
                    quantidade_estoque REAL,
                    unidade TEXT,
                    observacoes TEXT
                )
            """)
            
            # Verificar e adicionar coluna 'tipo' se não existir
            cursor.execute("PRAGMA table_info(lubrificantes)")
            cols = [c[1] for c in cursor.fetchall()]
            if 'tipo' not in cols:
                cursor.execute("ALTER TABLE lubrificantes ADD COLUMN tipo TEXT DEFAULT 'óleo'")
            
            # Tabela de almoxarifados
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS almoxarifados (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    nome TEXT NOT NULL,
                    tipo TEXT DEFAULT 'fixo', -- 'fixo' para oficina, 'movel' para caminhões
                    localizacao TEXT,
                    responsavel TEXT,
                    observacoes TEXT,
                    ativo BOOLEAN DEFAULT 1
                )
            """)
            
            # Tabela de estoque por almoxarifado
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS almoxarifado_estoque (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    id_almoxarifado INTEGER,
                    id_lubrificante INTEGER,
                    quantidade_estoque REAL DEFAULT 0,
                    unidade TEXT,
                    data_atualizacao TEXT,
                    FOREIGN KEY(id_almoxarifado) REFERENCES almoxarifados(id),
                    FOREIGN KEY(id_lubrificante) REFERENCES lubrificantes(id),
                    UNIQUE(id_almoxarifado, id_lubrificante)
                )
            """)
            
            # Tabela de movimentações (atualizada para incluir almoxarifado)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS lubrificantes_movimentacoes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    id_lubrificante INTEGER,
                    id_almoxarifado INTEGER,
                    tipo TEXT, -- 'entrada' ou 'saida'
                    quantidade REAL,
                    data TEXT,
                    cod_equip INTEGER,
                    observacoes TEXT,
                    FOREIGN KEY(id_lubrificante) REFERENCES lubrificantes(id),
                    FOREIGN KEY(id_almoxarifado) REFERENCES almoxarifados(id)
                )
            """)
            
            # Verificar se a coluna id_almoxarifado existe na tabela de movimentações
            cursor.execute("PRAGMA table_info(lubrificantes_movimentacoes)")
            cols_mov = [c[1] for c in cursor.fetchall()]
            if 'id_almoxarifado' not in cols_mov:
                cursor.execute("ALTER TABLE lubrificantes_movimentacoes ADD COLUMN id_almoxarifado INTEGER")
            
            conn.commit()
        return True, "Tabelas de lubrificantes e almoxarifados verificadas"
    except Exception as e:
        return False, f"Erro ao criar tabelas de lubrificantes: {e}"
    
def add_almoxarifado(nome, tipo="fixo", localizacao="", responsavel="", observacoes=""):
    """Adiciona um novo almoxarifado."""
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO almoxarifados (nome, tipo, localizacao, responsavel, observacoes) VALUES (?, ?, ?, ?, ?)",
                (nome, tipo, localizacao, responsavel, observacoes)
            )
            conn.commit()
        return True, "Almoxarifado cadastrado com sucesso!"
    except Exception as e:
        return False, f"Erro: {e}"

def get_almoxarifados():
    """Retorna todos os almoxarifados ativos."""
    try:
        with sqlite3.connect(DB_PATH) as conn:
            df = pd.read_sql("SELECT * FROM almoxarifados WHERE ativo = 1 ORDER BY nome", conn)
        return df
    except Exception as e:
        return pd.DataFrame()

def get_estoque_por_almoxarifado(id_lubrificante):
    """Retorna o estoque de um lubrificante distribuído por almoxarifados."""
    try:
        with sqlite3.connect(DB_PATH) as conn:
            query = """
            SELECT 
                a.nome as almoxarifado,
                a.tipo,
                COALESCE(ae.quantidade_estoque, 0) as quantidade,
                COALESCE(ae.unidade, l.unidade) as unidade,
                a.localizacao,
                a.responsavel
            FROM almoxarifados a
            CROSS JOIN lubrificantes l
            LEFT JOIN almoxarifado_estoque ae ON a.id = ae.id_almoxarifado AND l.id = ae.id_lubrificante
            WHERE l.id = ? AND a.ativo = 1
            ORDER BY a.nome
            """
            df = pd.read_sql(query, conn, params=(id_lubrificante,))
        return df
    except Exception as e:
        return pd.DataFrame()

def atualizar_estoque_almoxarifado(id_almoxarifado, id_lubrificante, quantidade, unidade):
    """Atualiza o estoque de um lubrificante em um almoxarifado específico."""
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT OR REPLACE INTO almoxarifado_estoque 
                (id_almoxarifado, id_lubrificante, quantidade_estoque, unidade, data_atualizacao) 
                VALUES (?, ?, ?, ?, ?)
            """, (id_almoxarifado, id_lubrificante, quantidade, unidade, date.today().strftime("%Y-%m-%d")))
            conn.commit()
        return True, "Estoque atualizado com sucesso!"
    except Exception as e:
        return False, f"Erro ao atualizar estoque: {e}"

def add_lubrificante(nome, viscosidade, quantidade, unidade, observacoes=""):
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO lubrificantes (nome, viscosidade, quantidade_estoque, unidade, observacoes) VALUES (?, ?, ?, ?, ?)",
                (nome, viscosidade, quantidade, unidade, observacoes)
            )
            conn.commit()
        return True, "Lubrificante cadastrado!"
    except Exception as e:
        return False, f"Erro: {e}"

def importar_lubrificantes_de_planilha(db_path: str, arquivo_carregado):
    """Importa lubrificantes de uma planilha Excel, verificando duplicatas."""
    try:
        df_lub = pd.read_excel(arquivo_carregado)
        df_lub.columns = [c.strip() for c in df_lub.columns]
        
        # Mapeamento de colunas
        mapa_colunas = {
            'nome': 'nome',
            'tipo': 'tipo',
            'viscosidade': 'viscosidade',
            'quantidade_estoque': 'quantidade_estoque',
            'unidade': 'unidade',
            'observacoes': 'observacoes'
        }
        
        # Normalizar nomes de colunas
        for col_orig, col_norm in mapa_colunas.items():
            if col_orig in df_lub.columns:
                df_lub = df_lub.rename(columns={col_orig: col_norm})
        
        # Verificar colunas obrigatórias
        obrig = ['nome']
        faltando = [c for c in obrig if c not in df_lub.columns]
        if faltando:
            return 0, 0, f"Colunas obrigatórias faltando: {', '.join(faltando)}"
        
        # Adicionar colunas opcionais se não existirem
        if 'tipo' not in df_lub.columns:
            df_lub['tipo'] = 'óleo'
        if 'viscosidade' not in df_lub.columns:
            df_lub['viscosidade'] = ''
        if 'quantidade_estoque' not in df_lub.columns:
            df_lub['quantidade_estoque'] = 0
        if 'unidade' not in df_lub.columns:
            df_lub['unidade'] = 'L'
        if 'observacoes' not in df_lub.columns:
            df_lub['observacoes'] = ''
        
        # Limpar e normalizar dados
        df_lub = df_lub.dropna(subset=['nome'])
        df_lub['nome'] = df_lub['nome'].astype(str).str.strip()
        df_lub['tipo'] = df_lub['tipo'].astype(str).str.strip().fillna('óleo')
        df_lub['viscosidade'] = df_lub['viscosidade'].astype(str).str.strip().fillna('')
        df_lub['quantidade_estoque'] = pd.to_numeric(df_lub['quantidade_estoque'], errors='coerce').fillna(0)
        df_lub['unidade'] = df_lub['unidade'].astype(str).str.strip().fillna('L')
        df_lub['observacoes'] = df_lub['observacoes'].astype(str).str.strip().fillna('')
        
        # Remover duplicatas na própria planilha baseada no nome
        df_lub = df_lub.drop_duplicates(subset=['nome'])
        
        with sqlite3.connect(db_path, check_same_thread=False) as conn:
            # Garantir que a tabela existe com a coluna tipo
            ensure_lubrificantes_schema()
            
            # Buscar lubrificantes existentes
            df_existente = pd.read_sql_query("SELECT nome FROM lubrificantes", conn)
            
            if not df_existente.empty:
                # Normalizar nomes existentes para comparação
                df_existente['nome'] = df_existente['nome'].astype(str).str.strip()
                
                # Filtrar apenas registros que não existem
                df_para_inserir = df_lub[~df_lub['nome'].isin(df_existente['nome'])]
            else:
                df_para_inserir = df_lub
            
            num_duplicados = len(df_lub) - len(df_para_inserir)
            
            if df_para_inserir.empty:
                return 0, num_duplicados, "Nenhum lubrificante novo para importar. Todos os registros da planilha já existem na base de dados."
            
            # Preparar registros para inserção
            colunas_insert = ['nome', 'tipo', 'viscosidade', 'quantidade_estoque', 'unidade', 'observacoes']
            df_para_inserir_final = df_para_inserir[colunas_insert]
            registros = [tuple(x) for x in df_para_inserir_final.to_numpy()]
            
            cur = conn.cursor()
            placeholders = ", ".join(["?"] * len(colunas_insert))
            sql = f"INSERT INTO lubrificantes ({', '.join(f'\"{col}\"' for col in colunas_insert)}) VALUES ({placeholders})"
            cur.executemany(sql, registros)
            conn.commit()
            
            num_inseridos = len(registros)
            
            mensagem_sucesso = f"{num_inseridos} lubrificantes novos foram importados com sucesso."
            if num_duplicados > 0:
                mensagem_sucesso += f" {num_duplicados} registros duplicados foram ignorados."
            
            return num_inseridos, num_duplicados, mensagem_sucesso
            
    except Exception as e:
        return 0, 0, f"Erro ao importar lubrificantes: {e}"

def importar_componentes_de_planilha(db_path: str, arquivo_carregado, classe_operacional: str):
    """Importa componentes de uma planilha Excel, verificando duplicatas e criando lubrificantes se necessário."""
    try:
        df_comp = pd.read_excel(arquivo_carregado)
        df_comp.columns = [c.strip() for c in df_comp.columns]
        
        # Mapeamento de colunas
        mapa_colunas = {
            'nome_componente': 'nome_componente',
            'componente': 'nome_componente',
            'intervalo_padrao': 'intervalo_padrao',
            'intervalo': 'intervalo_padrao',
            'lubrificante_nome': 'lubrificante_nome',
            'lubrificante': 'lubrificante_nome',
            'capacidade_litros': 'capacidade_litros',
            'capacidade': 'capacidade_litros'
        }
        
        # Normalizar nomes de colunas
        for col_orig, col_norm in mapa_colunas.items():
            if col_orig in df_comp.columns:
                df_comp = df_comp.rename(columns={col_orig: col_norm})
        
        # Verificar colunas obrigatórias
        obrig = ['nome_componente', 'intervalo_padrao']
        faltando = [c for c in obrig if c not in df_comp.columns]
        if faltando:
            return 0, 0, 0, f"Colunas obrigatórias faltando: {', '.join(faltando)}"
        
        # Adicionar colunas opcionais se não existirem
        if 'lubrificante_nome' not in df_comp.columns:
            df_comp['lubrificante_nome'] = None
        if 'capacidade_litros' not in df_comp.columns:
            df_comp['capacidade_litros'] = 0.0
        
        # Limpar e normalizar dados
        df_comp = df_comp.dropna(subset=['nome_componente'])
        df_comp['nome_componente'] = df_comp['nome_componente'].astype(str).str.strip()
        df_comp['intervalo_padrao'] = pd.to_numeric(df_comp['intervalo_padrao'], errors='coerce')
        df_comp = df_comp.dropna(subset=['intervalo_padrao'])
        df_comp['lubrificante_nome'] = df_comp['lubrificante_nome'].astype(str).str.strip().replace('nan', None)
        df_comp['capacidade_litros'] = pd.to_numeric(df_comp['capacidade_litros'], errors='coerce').fillna(0.0)
        
        # Remover duplicatas na própria planilha baseada no nome do componente
        df_comp = df_comp.drop_duplicates(subset=['nome_componente'])
        
        with sqlite3.connect(db_path, check_same_thread=False) as conn:
            # Garantir que as tabelas existem
            ensure_lubrificantes_schema()
            
            # Verificar se a tabela componentes_regras tem a coluna capacidade_litros
            cursor = conn.cursor()
            cursor.execute("PRAGMA table_info(componentes_regras)")
            columns = [column[1] for column in cursor.fetchall()]
            
            if 'capacidade_litros' not in columns:
                cursor.execute("ALTER TABLE componentes_regras ADD COLUMN capacidade_litros REAL DEFAULT 0.0")
            
            # Buscar componentes existentes na classe
            df_existente = pd.read_sql_query(
                "SELECT nome_componente FROM componentes_regras WHERE classe_operacional = ?", 
                conn, params=(classe_operacional,)
            )
            
            if not df_existente.empty:
                # Normalizar nomes existentes para comparação
                df_existente['nome_componente'] = df_existente['nome_componente'].astype(str).str.strip()
                
                # Filtrar apenas registros que não existem na classe
                df_para_inserir = df_comp[~df_comp['nome_componente'].isin(df_existente['nome_componente'])]
            else:
                df_para_inserir = df_comp
            
            num_duplicados = len(df_comp) - len(df_para_inserir)
            
            if df_para_inserir.empty:
                return 0, num_duplicados, 0, "Nenhum componente novo para importar. Todos os registros da planilha já existem na classe selecionada."
            
            # Processar lubrificantes
            lubrificantes_criados = 0
            for _, row in df_para_inserir.iterrows():
                if pd.notna(row['lubrificante_nome']) and row['lubrificante_nome']:
                    # Verificar se o lubrificante existe
                    df_lub_existente = pd.read_sql_query(
                        "SELECT id FROM lubrificantes WHERE nome = ?", 
                        conn, params=(row['lubrificante_nome'],)
                    )
                    
                    if df_lub_existente.empty:
                        # Criar lubrificante automaticamente
                        cur = conn.cursor()
                        cur.execute(
                            "INSERT INTO lubrificantes (nome, tipo, viscosidade, quantidade_estoque, unidade, observacoes) VALUES (?, ?, ?, ?, ?, ?)",
                            (row['lubrificante_nome'], 'óleo', '', 0, 'L', f'Criado automaticamente durante importação de componentes')
                        )
                        lubrificantes_criados += 1
                        
                        # Buscar o ID do lubrificante criado
                        lub_id = cur.lastrowid
                    else:
                        lub_id = df_lub_existente.iloc[0]['id']
                else:
                    lub_id = None
                
                # Inserir componente
                cur = conn.cursor()
                cur.execute(
                    "INSERT INTO componentes_regras (classe_operacional, nome_componente, intervalo_padrao, lubrificante_id, capacidade_litros) VALUES (?, ?, ?, ?, ?)",
                    (classe_operacional, row['nome_componente'], row['intervalo_padrao'], lub_id, row['capacidade_litros'])
                )
            
            conn.commit()
            num_inseridos = len(df_para_inserir)
            
            mensagem_sucesso = f"{num_inseridos} componentes foram importados com sucesso para a classe '{classe_operacional}'."
            if num_duplicados > 0:
                mensagem_sucesso += f" {num_duplicados} componentes duplicados foram ignorados."
            if lubrificantes_criados > 0:
                mensagem_sucesso += f" {lubrificantes_criados} lubrificantes foram criados automaticamente."
            
            return num_inseridos, num_duplicados, lubrificantes_criados, mensagem_sucesso
            
    except Exception as e:
        return 0, 0, 0, f"Erro ao importar componentes: {e}"

def movimentar_lubrificante(id_lubrificante, tipo, quantidade, data, cod_equip=None, observacoes=""):
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO lubrificantes_movimentacoes (id_lubrificante, tipo, quantidade, data, cod_equip, observacoes) VALUES (?, ?, ?, ?, ?, ?)",
                (id_lubrificante, tipo, quantidade, data, cod_equip, observacoes)
            )
            # Atualiza estoque
            sinal = 1 if tipo == "entrada" else -1
            cur.execute(
                "UPDATE lubrificantes SET quantidade_estoque = quantidade_estoque + ? WHERE id = ?",
                (sinal * quantidade, id_lubrificante)
            )
            conn.commit()
        return True, "Movimentação registrada!"
    except Exception as e:
        return False, f"Erro: {e}"

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
        # Primeira tentativa: usar conexão direta
        conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        cursor = conn.cursor()
        
        # Converter tipos de dados para garantir compatibilidade
        cod_equip = int(cod_equip)  # Converter numpy.int64 para int
        titulo_checklist = str(titulo_checklist)
        data_preenchimento = str(data_preenchimento)
        turno = str(turno)
        
        # Debug: verificar todos os registros na tabela ANTES da exclusão
        cursor.execute("SELECT rowid, Cod_Equip, titulo_checklist, data_preenchimento, turno FROM checklist_historico")
        all_records_before = cursor.fetchall()
        
        # Tentar encontrar o registro com diferentes abordagens
        rowid = None
        
        # Primeira tentativa: busca exata
        cursor.execute(
            "SELECT rowid FROM checklist_historico WHERE Cod_Equip = ? AND titulo_checklist = ? AND data_preenchimento = ? AND turno = ?", 
            (cod_equip, titulo_checklist, data_preenchimento, turno)
        )
        result = cursor.fetchone()
        
        if result:
            rowid = result[0]
        else:
            # Segunda tentativa: buscar apenas por Cod_Equip, título e turno (ignorar data)
            cursor.execute(
                "SELECT rowid FROM checklist_historico WHERE Cod_Equip = ? AND titulo_checklist = ? AND turno = ?", 
                (cod_equip, titulo_checklist, turno)
            )
            result = cursor.fetchone()
            
            if result:
                rowid = result[0]
            else:
                # Terceira tentativa: buscar apenas por Cod_Equip e título
                cursor.execute(
                    "SELECT rowid FROM checklist_historico WHERE Cod_Equip = ? AND titulo_checklist = ?", 
                    (cod_equip, titulo_checklist)
                )
                result = cursor.fetchone()
                
                if result:
                    rowid = result[0]
        
        if rowid is None:
            # Debug: retornar informações sobre o que foi encontrado
            debug_info = f"""
            Registro não encontrado para exclusão.
            
            Valores procurados (após conversão):
            - Cod_Equip: {cod_equip} (tipo: {type(cod_equip)})
            - Título: {titulo_checklist} (tipo: {type(titulo_checklist)})
            - Data: {data_preenchimento} (tipo: {type(data_preenchimento)})
            - Turno: {turno} (tipo: {type(turno)})
            
            Todos os registros na tabela ANTES da exclusão:
            {all_records_before}
            """
            conn.close()
            return False, debug_info
        
        # Agora vamos excluir usando rowid
        cursor.execute("DELETE FROM checklist_historico WHERE rowid = ?", (rowid,))
        
        # Forçar commit imediato
        conn.commit()
        
        # Verificar se foi realmente excluído
        rows_deleted = cursor.rowcount
        if rows_deleted > 0:
            # Verificar novamente se o registro foi realmente excluído
            cursor.execute("SELECT COUNT(*) FROM checklist_historico WHERE rowid = ?", (rowid,))
            count_after = cursor.fetchone()[0]
            
            # Verificar também se o registro ainda existe pelos outros campos
            cursor.execute(
                "SELECT COUNT(*) FROM checklist_historico WHERE Cod_Equip = ? AND titulo_checklist = ? AND data_preenchimento = ? AND turno = ?", 
                (cod_equip, titulo_checklist, data_preenchimento, turno)
            )
            count_by_fields = cursor.fetchone()[0]
            
            if count_after == 0 and count_by_fields == 0:
                 # Verificar o total de registros na tabela
                 cursor.execute("SELECT COUNT(*) FROM checklist_historico")
                 total_after = cursor.fetchone()[0]
                 
                 # Forçar sincronização do banco
                 cursor.execute("PRAGMA wal_checkpoint(FULL)")
                 cursor.execute("PRAGMA synchronous=FULL")
                 conn.commit()
                 
                 success_msg = f"Checklist excluído com sucesso! ({rows_deleted} registro(s) removido(s)). Total na tabela: {total_after}"
                 
                 # Salvar backup automático para persistência no Streamlit Cloud
                 backup_success, backup_msg = save_backup_to_session_state()
                 if backup_success:
                     success_msg += f" | Backup salvo: {backup_msg}"
                 else:
                     success_msg += f" | Aviso: {backup_msg}"
                 
                 conn.close()
                 return True, success_msg
            else:
                conn.close()
                return False, f"Erro: Registro ainda existe após exclusão. Count by rowid: {count_after}, Count by fields: {count_by_fields}"
        else:
            conn.close()
            return False, "Nenhum registro foi excluído"
                
    except Exception as e:
        if 'conn' in locals():
            conn.close()
        return False, f"Erro ao excluir checklist: {e}"


def force_cache_clear():
    """Força a limpeza completa de todos os caches."""
    try:
        # Limpar cache de dados
        st.cache_data.clear()
        
        # Limpar cache de recursos
        st.cache_resource.clear()
        
        # Forçar rerun da aplicação
        st.rerun()
    except Exception as e:
        st.error(f"Erro ao limpar cache: {e}")


def force_database_sync():
    """Força a sincronização do banco de dados com o disco."""
    try:
        conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        cursor = conn.cursor()
        
        # Forçar commit
        conn.commit()
        
        # Executar PRAGMA para forçar sincronização
        cursor.execute("PRAGMA wal_checkpoint(FULL)")
        cursor.execute("PRAGMA synchronous=FULL")
        cursor.execute("PRAGMA journal_mode=DELETE")
        
        # Forçar commit novamente
        conn.commit()
        
        # Verificar se o banco está em modo WAL
        cursor.execute("PRAGMA journal_mode")
        journal_mode = cursor.fetchone()[0]
        
        conn.close()
        
        return True, f"Banco sincronizado. Modo journal: {journal_mode}"
    except Exception as e:
        return False, f"Erro ao sincronizar banco: {e}"


def export_database_backup():
    """Exporta todos os dados do banco para um arquivo de backup."""
    try:
        conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        
        # Obter todas as tabelas
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        
        backup_data = {}
        
        for table in tables:
            table_name = table[0]
            if table_name != 'sqlite_master':
                # Exportar dados da tabela
                df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
                backup_data[table_name] = df.to_dict('records')
        
        conn.close()
        
        # Converter para JSON
        backup_json = json.dumps(backup_data, default=str, indent=2)
        
        # Criar arquivo de download
        backup_bytes = backup_json.encode('utf-8')
        backup_b64 = base64.b64encode(backup_bytes).decode()
        
        return backup_b64, backup_data
        
    except Exception as e:
        return None, f"Erro ao exportar backup: {e}"


def import_database_backup(backup_data):
    """Importa dados de backup para o banco."""
    try:
        conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        cursor = conn.cursor()
        
        for table_name, records in backup_data.items():
            if records:  # Se a tabela tem dados
                # Limpar tabela existente
                cursor.execute(f"DELETE FROM {table_name}")
                
                # Inserir novos dados
                for record in records:
                    columns = list(record.keys())
                    placeholders = ', '.join(['?' for _ in columns])
                    values = list(record.values())
                    
                    # Converter tipos de dados
                    converted_values = []
                    for value in values:
                        if isinstance(value, str):
                            # Tentar converter para datetime se for uma data
                            try:
                                if 'T' in value or '-' in value:
                                    dt = pd.to_datetime(value)
                                    converted_values.append(dt.strftime('%Y-%m-%d %H:%M:%S'))
                                else:
                                    converted_values.append(value)
                            except:
                                converted_values.append(value)
                        else:
                            converted_values.append(value)
                    
                    cursor.execute(
                        f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})",
                        converted_values
                    )
        
        conn.commit()
        conn.close()
        
        return True, "Backup restaurado com sucesso!"
        
    except Exception as e:
        return False, f"Erro ao restaurar backup: {e}"


def save_backup_to_session_state():
    """Salva backup dos dados na sessão do Streamlit."""
    try:
        backup_b64, backup_data = export_database_backup()
        if backup_b64:
            st.session_state['database_backup'] = backup_b64
            st.session_state['backup_timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            return True, "Backup salvo na sessão"
        else:
            return False, "Erro ao criar backup"
    except Exception as e:
        return False, f"Erro ao salvar backup: {e}"


def restore_backup_from_session_state():
    """Restaura backup dos dados da sessão do Streamlit."""
    try:
        if 'database_backup' in st.session_state:
            backup_b64 = st.session_state['database_backup']
            backup_bytes = base64.b64decode(backup_b64)
            backup_json = backup_bytes.decode('utf-8')
            backup_data = json.loads(backup_json)
            
            success, message = import_database_backup(backup_data)
            if success:
                # Limpar cache para forçar recarregamento
                force_cache_clear()
                return True, message
            else:
                return False, message
        else:
            return False, "Nenhum backup encontrado na sessão"
    except Exception as e:
        return False, f"Erro ao restaurar backup: {e}"


def auto_restore_backup_on_startup():
    """Tenta restaurar backup automaticamente na inicialização da aplicação."""
    try:
        if 'database_backup' in st.session_state:
            # Verificar se o banco está vazio
            conn = sqlite3.connect(DB_PATH, check_same_thread=False)
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table'")
            num_tables = cursor.fetchone()[0]
            conn.close()
            
            if num_tables == 0:
                # Banco vazio, tentar restaurar
                success, message = restore_backup_from_session_state()
                if success:
                    st.info("🔄 Backup restaurado automaticamente na inicialização!")
                    return True
                else:
                    st.warning(f"⚠️ Falha na restauração automática: {message}")
                    return False
        return False
    except Exception as e:
        st.warning(f"⚠️ Erro na restauração automática: {e}")
        return False


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
            col_logo, col_title = st.columns([1, 8])
            with col_logo:
                st.image("logo.png", width=80)
            with col_title:
                st.title("📊 Dashboard de Frotas e Abastecimentos")
        else:
            st.title("📊 Dashboard de Frotas e Abastecimentos")

        # Tentar restaurar backup automaticamente na inicialização
        auto_restore_backup_on_startup()
        
        # Adicionar coluna de tipo de combustível se não existir
        add_tipo_combustivel_column()
        
        # Setup de esquemas (motoristas, preços, combustível)
        ensure_motoristas_schema()
        ensure_precos_combustivel_schema()

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
                st.image("logo.png", width=200)
            st.write(f"Bem-vindo, **{st.session_state.username}**!")
            if st.button("Sair"):
                st.session_state.authenticated = False
                st.session_state.username = "" # Limpa o username ao sair
                st.session_state.role = None
                st.rerun()
            st.markdown("---")

        with st.sidebar:
            st.header("📅 Filtros (válidos apenas na aba Análise Geral)")

            # Persistência de período
            if 'filtro_data_inicio' not in st.session_state:
                st.session_state['filtro_data_inicio'] = df['Data'].min().date()
            if 'filtro_data_fim' not in st.session_state:
                st.session_state['filtro_data_fim'] = df['Data'].max().date()

            st.subheader("Período de Análise")
            data_inicio = st.date_input(
                "Data de Início", 
                st.session_state['filtro_data_inicio'],
                key='data_inicio'
            )
            data_fim = st.date_input(
                "Data de Fim", 
                st.session_state['filtro_data_fim'],
                key='data_fim'
            )
            st.session_state['filtro_data_inicio'] = data_inicio
            st.session_state['filtro_data_fim'] = data_fim

            st.markdown("---")
            st.caption("Desenvolvido por André Luis")

            with st.expander("Filtrar por Classe Operacional"):
                classe_opts = sorted(list(df["Classe_Operacional"].dropna().unique()))
                sel_classes = st.multiselect(
                    "Selecione as Classes", 
                    classe_opts, 
                    default=classe_opts,
                    key="sel_classes"
                )

            with st.expander("Filtrar por Safra"):
                safra_opts = sorted(list(df["Safra"].dropna().unique()))
                sel_safras = st.multiselect(
                    "Selecione as Safras", 
                    safra_opts, 
                    default=safra_opts,
                    key="sel_safras"
                )

            # Só aplicaremos os filtros na aba "📈 Análise Geral" (guardaremos em sessão)
            st.session_state['filtro_opts_analise'] = {
                "data_inicio": data_inicio,
                "data_fim": data_fim,
                "classes_op": sel_classes,
                "safras": sel_safras
            }
    #----------------------------------------------------- aba principal --------------------------------------
        # df_f será calculado apenas para a aba Análise Geral
        df_f = None
        plan_df = build_component_maintenance_plan(df_frotas, df, df_comp_regras, df_comp_historico)


        # CSS para barra de rolagem horizontal nas abas com design moderno
        st.markdown("""
        <style>
        .stTabs [data-baseweb="tab-list"] {
            gap: 8px;
            overflow-x: auto;
            scrollbar-width: thin;
            scrollbar-color: #00D4AA #E8F5F2;
            padding: 8px 0;
        }
        
        .stTabs [data-baseweb="tab-list"]::-webkit-scrollbar {
            height: 12px;
        }
        
        .stTabs [data-baseweb="tab-list"]::-webkit-scrollbar-track {
            background: linear-gradient(90deg, #F0F2F6 0%, #E8F5F2 100%);
            border-radius: 8px;
            border: 1px solid #E0E6ED;
            box-shadow: inset 0 1px 3px rgba(0,0,0,0.1);
        }
        
        .stTabs [data-baseweb="tab-list"]::-webkit-scrollbar-thumb {
            background: linear-gradient(90deg, #00D4AA 0%, #00B8A9 100%);
            border-radius: 8px;
            border: 1px solid #00A896;
            box-shadow: 0 2px 4px rgba(0,212,170,0.3);
            transition: all 0.3s ease;
        }
        
        .stTabs [data-baseweb="tab-list"]::-webkit-scrollbar-thumb:hover {
            background: linear-gradient(90deg, #00B8A9 0%, #00A896 100%);
            box-shadow: 0 3px 6px rgba(0,212,170,0.4);
            transform: translateY(-1px);
        }
        
        .stTabs [data-baseweb="tab-list"]::-webkit-scrollbar-thumb:active {
            background: linear-gradient(90deg, #00A896 0%, #009688 100%);
            box-shadow: 0 1px 3px rgba(0,212,170,0.5);
        }
        
        .stTabs [data-baseweb="tab-list"] > div {
            flex-shrink: 0;
            transition: all 0.2s ease;
        }
        
        /* Melhorar aparência das abas */
        .stTabs [data-baseweb="tab"] {
            border-radius: 8px;
            transition: all 0.2s ease;
        }
        
        .stTabs [data-baseweb="tab"]:hover {
            transform: translateY(-1px);
            box-shadow: 0 2px 8px rgba(0,0,0,0.1);
        }
        
        /* Estilo para abas ativas */
        .stTabs [data-baseweb="tab"][aria-selected="true"] {
            background: linear-gradient(135deg, #00D4AA 0%, #00B8A9 100%);
            color: white;
            font-weight: 600;
            box-shadow: 0 4px 12px rgba(0,212,170,0.3);
        }
        </style>
        """, unsafe_allow_html=True)
        
        # Definição dos grupos de abas
        abas_pagina_inicial = ["📈 Análise Geral", "🛠️ Controle de Manutenção", "🔎 Consulta Individual", "✅ Checklists Diários"]
        abas_gerir = ["⚙️ Gerir Lançamentos", "🛢️ Gestão de Lubrificantes", "⚙️ Gerir Frotas", "✅ Gerir Checklists"]
        abas_dados = ["📤 Importar Dados", "⚕️ Saúde dos Dados", "💾 Backup", "👤 Gerir Utilizadores", "⚙️ Configurações"]

        # Sistema de navegação por grupos
        st.markdown("### 🎯 Navegação por Grupos")
        
        # Mostrar informações sobre os grupos disponíveis
        if st.session_state.role == 'admin':
            st.info("""
            **🏠 Página Inicial:** Visualizações, análises e consultas principais | 
            **⚙️ Gerir:** Gestão de lançamentos, frotas e checklists | 
            **📊 Dados:** Importações, backup e saúde dos dados
            """)
        else:
            st.info("**🏠 Página Inicial:** Visualizações, análises e consultas principais")
        
        # CSS para os botões de grupo
        st.markdown("""
        <style>
        .group-nav-button {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            border: none;
            border-radius: 12px;
            padding: 12px 20px;
            font-weight: 600;
            font-size: 14px;
            transition: all 0.3s ease;
            box-shadow: 0 4px 12px rgba(102,126,234,0.3);
            margin: 4px;
            cursor: pointer;
        }
        
        .group-nav-button:hover {
            background: linear-gradient(135deg, #764ba2 0%, #667eea 100%);
            transform: translateY(-2px);
            box-shadow: 0 6px 16px rgba(102,126,234,0.4);
        }
        
        .group-nav-button.active {
            background: linear-gradient(135deg, #00D4AA 0%, #00B8A9 100%);
            box-shadow: 0 4px 12px rgba(0,212,170,0.3);
        }
        
        .group-section {
            background: linear-gradient(135deg, #f8f9fa 0%, #e9ecef 100%);
            border-radius: 12px;
            padding: 16px;
            margin: 8px 0;
            border: 1px solid #dee2e6;
        }
        </style>
        """, unsafe_allow_html=True)

        # Botões de navegação por grupos
        col1, col2, col3 = st.columns(3)
        
        # Determinar grupo ativo
        active_group = st.session_state.get('active_group', 'pagina_inicial').strip().lower()

        with col1:
            if st.button("🏠 Página Inicial", key="nav_pagina_inicial", help="Visualizações e análises principais"):
                st.session_state['active_group'] = 'pagina_inicial'
                st.session_state['active_tab_index'] = 0
                st.rerun()
        
        if st.session_state.role == 'admin':
            with col2:
                if st.button("⚙️ Gerir", key="nav_gerir", help="Gestão de lançamentos, frotas e checklists"):
                    st.session_state['active_group'] = 'gerir'
                    st.session_state['active_tab_index'] = 0
                    st.rerun()
            
            with col3:
                if st.button("📊 Dados", key="nav_dados", help="Importações, backup e saúde dos dados"):
                    st.session_state['active_group'] = 'dados'
                    st.session_state['active_tab_index'] = 0
                    st.rerun()

        # Determinar quais abas mostrar baseado no grupo ativo
        if st.session_state.role == 'admin':
            if active_group == 'pagina_inicial':
                tabs_para_mostrar = abas_pagina_inicial
            elif active_group == 'gerir':
                tabs_para_mostrar = abas_gerir
            elif active_group == 'dados':
                tabs_para_mostrar = abas_dados
            else:
                tabs_para_mostrar = abas_pagina_inicial
        else:
            # Para usuários comuns, mostrar apenas página inicial
            tabs_para_mostrar = abas_pagina_inicial

        # Determinar índice ativo
        active_idx = st.session_state.get('active_tab_index', 0)
        active_idx = max(0, min(active_idx, len(tabs_para_mostrar) - 1))
        
        # Criar as abas
        try:
            abas = st.tabs(tabs_para_mostrar, default_index=active_idx)
        except TypeError:
            abas = st.tabs(tabs_para_mostrar)

        # Atribuir as abas baseado no grupo ativo
        if st.session_state.role == 'admin':
            if active_group == 'pagina_inicial':
                tab_analise, tab_manut, tab_consulta, tab_checklists = abas
                # Criar variáveis vazias para as outras abas
                tab_gerir_lanc = tab_gerir_lub = tab_gerir_frotas = tab_gerir_checklists = None
                tab_importar = tab_saude = tab_backup = tab_gerir_users = tab_config = None
            elif active_group == 'gerir':
                tab_gerir_lanc, tab_gerir_lub, tab_gerir_frotas, tab_gerir_checklists = abas
                # Criar variáveis vazias para as outras abas
                tab_analise = tab_manut = tab_consulta = tab_checklists = None
                tab_importar = tab_saude = tab_backup = tab_gerir_users = tab_config = None
            elif active_group == 'dados':
                tab_importar, tab_saude, tab_backup, tab_gerir_users, tab_config = abas
                # Criar variáveis vazias para as outras abas
                tab_analise = tab_manut = tab_consulta = tab_checklists = None
                tab_gerir_lanc = tab_gerir_lub = tab_gerir_frotas = tab_gerir_checklists = None
                

        else:
            tab_analise, tab_manut, tab_consulta, tab_checklists = abas
            # Criar variáveis vazias para as outras abas
            tab_gerir_lanc = tab_gerir_lub = tab_gerir_frotas = tab_gerir_checklists = None
            tab_importar = tab_saude = tab_backup = tab_gerir_users = tab_config = None

        def rerun_keep_tab(tab_title: str, clear_cache: bool = True):
            if clear_cache:
                st.cache_data.clear()
            try:
                st.session_state['active_tab_index'] = tabs_para_mostrar.index(tab_title)
            except Exception:
                pass
            st.rerun()
        

                
        if tab_analise is not None:
            with tab_analise:
                st.header("📈 Análise Gráfica de Consumo")

                # Aplica filtros apenas nesta aba
                opts = st.session_state.get('filtro_opts_analise', None)
                df_f = filtrar_dados(df, opts) if opts else df.copy()

                if not df_f.empty:
                    if 'Media' in df_f.columns:
                        k1, k2 = st.columns(2)
                        k1.metric("Litros Consumidos (período)", formatar_brasileiro_int(df_f["Qtde_Litros"].sum()))
                        k2.metric("Média Consumo (período)", f"{formatar_brasileiro(df_f['Media'].mean())}")
                    else:
                        k1.metric("Litros Consumidos (período)", formatar_brasileiro_int(df_f["Qtde_Litros"].sum()))
                    st.markdown("---")
                    st.subheader("📊 Análise de Consumo por Classe e Equipamentos")
                    c1, c2 = st.columns(2)

                    with c1:
                        st.subheader("Consumo por Classe Operacional")
                        classes_a_excluir = ['VEICULOS LEVES', 'MOTOCICLETA', 'MINI CARREGADEIRA', 'USINA']
                        # Verificar se a coluna Classe_Operacional existe antes de filtrar
                        if 'Classe_Operacional' in df_f.columns:
                            df_consumo_classe = df_f[~df_f['Classe_Operacional'].str.upper().isin(classes_a_excluir)]
                        else:
                            df_consumo_classe = df_f
                        consumo_por_classe = df_consumo_classe.groupby("Classe_Operacional")["Qtde_Litros"].sum().sort_values(ascending=False).reset_index()

                        if not consumo_por_classe.empty:
                            consumo_por_classe['texto_formatado'] = consumo_por_classe['Qtde_Litros'].apply(formatar_brasileiro_int)
                            fig_classe = px.bar(consumo_por_classe, x='Qtde_Litros', y='Classe_Operacional', orientation='h', text='texto_formatado', labels={"x": "Litros Consumidos", "y": "Classe Operacional"})
                            fig_classe.update_traces(texttemplate='%{text} L', textposition='outside')
                            fig_classe.update_layout(yaxis={'categoryorder':'total ascending'}, xaxis_title="Total Consumido (Litros)", yaxis_title="Classe Operacional")
                            st.plotly_chart(fig_classe, use_container_width=True)

                    with c2:
                        st.subheader("Top 10 Equipamentos com Maior Consumo")
                        # Melhorar o gráfico com informações mais claras
                        consumo_por_equip = df_f.groupby("Cod_Equip").agg({'Qtde_Litros': 'sum'}).dropna()
                        consumo_por_equip = consumo_por_equip[consumo_por_equip.index != 550]
                        consumo_por_equip = consumo_por_equip.sort_values(by="Qtde_Litros", ascending=False).head(10)

                        if not consumo_por_equip.empty:
                            # Adicionar informações da frota para melhor identificação
                            consumo_por_equip = consumo_por_equip.reset_index()
                            consumo_por_equip = consumo_por_equip.merge(
                                df_frotas[['Cod_Equip', 'DESCRICAO_EQUIPAMENTO', 'PLACA']], 
                                on='Cod_Equip', 
                                how='left'
                            )
                            
                            # Criar label mais informativo: Código - Descrição (Placa)
                            consumo_por_equip['label_grafico'] = consumo_por_equip.apply(
                                lambda row: f"{row['Cod_Equip']} - {row['DESCRICAO_EQUIPAMENTO'][:20]}{'...' if len(str(row['DESCRICAO_EQUIPAMENTO'])) > 20 else ''} ({row['PLACA']})", 
                                axis=1
                            )
                            
                            consumo_por_equip['texto_formatado'] = consumo_por_equip['Qtde_Litros'].apply(formatar_brasileiro_int)
                            
                            fig_top10 = px.bar(
                                consumo_por_equip, 
                                x='Qtde_Litros', 
                                y='label_grafico', 
                                orientation='h', 
                                text='texto_formatado', 
                                labels={"Qtde_Litros": "Total Consumido (Litros)", "label_grafico": "Equipamento"},
                                title="Top 10 Equipamentos com Maior Consumo"
                            )
                            fig_top10.update_traces(
                                texttemplate='%{text} L', 
                                textposition='outside',
                                marker_color='#ff7f0e'
                            )
                            fig_top10.update_layout(
                                yaxis={'categoryorder':'total ascending'}, 
                                xaxis_title="Total Consumido (Litros)", 
                                yaxis_title="Equipamento",
                                height=400
                            )
                            st.plotly_chart(fig_top10, use_container_width=True)

                    st.markdown("---")
                    
                    # NOVA SEÇÃO: Top 10 de Gastos por Frota e por Classe
                    st.subheader("💰 Top 10 de Gastos por Frota e Classe")
                    
                    # Calcular gastos por frota
                    precos_map = get_precos_combustivel_map()
                    if precos_map:
                        df_gastos = df_f.copy()
                        
                        # Verificar se a coluna tipo_combustivel existe em df_frotas
                        if 'tipo_combustivel' in df_frotas.columns:
                            df_gastos = df_gastos.merge(df_frotas[['Cod_Equip','tipo_combustivel']], on='Cod_Equip', how='left')
                            # Verificar se a coluna foi criada após o merge
                            if 'tipo_combustivel' in df_gastos.columns:
                                df_gastos['tipo_combustivel'] = df_gastos['tipo_combustivel'].fillna('Diesel S500')
                            else:
                                df_gastos['tipo_combustivel'] = 'Diesel S500'
                        else:
                            # Se não existir, criar a coluna com valor padrão
                            df_gastos['tipo_combustivel'] = 'Diesel S500'
                        
                        # Garantir que a coluna tipo_combustivel existe antes de mapear preços
                        if 'tipo_combustivel' not in df_gastos.columns:
                            df_gastos['tipo_combustivel'] = 'Diesel S500'
                        
                        df_gastos['preco_unit'] = df_gastos['tipo_combustivel'].map(precos_map).fillna(0.0)
                        df_gastos['custo'] = df_gastos['Qtde_Litros'].fillna(0.0) * df_gastos['preco_unit']
                        
                        # Adicionar informações da frota para filtro
                        df_gastos_com_info = df_gastos.merge(
                            df_frotas[['Cod_Equip', 'DESCRICAO_EQUIPAMENTO', 'PLACA', 'Classe_Operacional']], 
                            on='Cod_Equip', 
                            how='left'
                        )
                        
                        # Garantir que a coluna Classe_Operacional existe
                        if 'Classe_Operacional' not in df_gastos_com_info.columns:
                            df_gastos_com_info['Classe_Operacional'] = 'N/A'
                        
                        # Filtro para excluir a frota 550 (usina) por padrão
                        mostrar_usinas = st.checkbox("🏭 Incluir Frota 550 (Usina) no Top 10 de Gastos por Frota", value=False)
                        
                        if not mostrar_usinas:
                            # Excluir a frota 550 (usina) do DataFrame
                            df_gastos_filtrado = df_gastos_com_info[df_gastos_com_info['Cod_Equip'] != 550]
                        else:
                            df_gastos_filtrado = df_gastos_com_info
                        
                        # Top 10 gastos por frota individual (após filtro)
                        gastos_por_frota = df_gastos_filtrado.groupby('Cod_Equip').agg({
                            'custo': 'sum',
                            'Qtde_Litros': 'sum'
                        }).sort_values('custo', ascending=False).head(10).reset_index()
                        
                        # Adicionar informações da frota
                        gastos_por_frota = gastos_por_frota.merge(
                            df_frotas[['Cod_Equip', 'DESCRICAO_EQUIPAMENTO', 'PLACA']], 
                            on='Cod_Equip', 
                            how='left'
                        )
                        gastos_por_frota['label_frota'] = gastos_por_frota.apply(
                            lambda row: f"{row['Cod_Equip']} - {row['DESCRICAO_EQUIPAMENTO'][:15]}{'...' if len(str(row['DESCRICAO_EQUIPAMENTO'])) > 15 else ''}", 
                            axis=1
                        )
                        gastos_por_frota['custo_formatado'] = gastos_por_frota['custo'].apply(lambda x: formatar_brasileiro(x, 'R$ '))
                        
                        # Top 10 gastos por classe operacional
                        gastos_por_classe = df_gastos.groupby('Classe_Operacional').agg({
                            'custo': 'sum',
                            'Qtde_Litros': 'sum'
                        }).sort_values('custo', ascending=False).head(10).reset_index()
                        gastos_por_classe['custo_formatado'] = gastos_por_classe['custo'].apply(lambda x: formatar_brasileiro(x, 'R$ '))
                        
                        # Criar layout em 2 colunas para os gráficos
                        col_gastos1, col_gastos2 = st.columns(2)
                        
                        with col_gastos1:
                            st.subheader("🏭 Top 10 Gastos por Frota")
                            
                            # Mostrar informação sobre filtro da frota 550
                            # Comentário removido para manter proporção dos gráficos
                            
                            if not gastos_por_frota.empty:
                                fig_gastos_frota = px.bar(
                                    gastos_por_frota,
                                    x='custo',
                                    y='label_frota',
                                    orientation='h',
                                    text='custo_formatado',
                                    title="Gastos por Frota Individual",
                                    labels={'custo': 'Custo (R$)', 'label_frota': 'Frota'},
                                    color='custo',
                                    color_continuous_scale='Reds'
                                )
                                fig_gastos_frota.update_traces(
                                    textposition='outside',
                                    texttemplate='%{text}'
                                )
                                fig_gastos_frota.update_layout(
                                    yaxis={'categoryorder':'total ascending'},
                                    xaxis_title="Custo Total (R$)",
                                    yaxis_title="Frota",
                                    height=400,
                                    showlegend=False
                                )
                                st.plotly_chart(fig_gastos_frota, use_container_width=True)
                            else:
                                st.info("Não há dados de gastos por frota.")
                        
                        with col_gastos2:
                            st.subheader("🏗️ Top 10 Gastos por Classe")
                            if not gastos_por_classe.empty:
                                fig_gastos_classe = px.bar(
                                    gastos_por_classe,
                                    x='custo',
                                    y='Classe_Operacional',
                                    orientation='h',
                                    text='custo_formatado',
                                    title="Gastos por Classe Operacional",
                                    labels={'custo': 'Custo (R$)', 'Classe_Operacional': 'Classe'},
                                    color='custo',
                                    color_continuous_scale='Blues'
                                )
                                fig_gastos_classe.update_traces(
                                    textposition='outside',
                                    texttemplate='%{text}'
                                )
                                fig_gastos_classe.update_layout(
                                    yaxis={'categoryorder':'total ascending'},
                                    xaxis_title="Custo Total (R$)",
                                    yaxis_title="Classe Operacional",
                                    height=400,
                                    showlegend=False
                                )
                                st.plotly_chart(fig_gastos_classe, use_container_width=True)
                            else:
                                st.info("Não há dados de gastos por classe.")
                        
                        # Resumo dos totais
                        st.markdown("---")
                        col_resumo1, col_resumo2, col_resumo3 = st.columns(3)
                        with col_resumo1:
                            st.metric(
                                "Total Gastos (Período)", 
                                formatar_brasileiro(df_gastos['custo'].sum(), 'R$ ')
                            )
                        with col_resumo2:
                            if not gastos_por_frota.empty:
                                frota_maior_gasto = gastos_por_frota.iloc[0]
                                st.metric(
                                    "Frota com Maior Gasto", 
                                    f"{frota_maior_gasto['Cod_Equip']}",
                                    f"{frota_maior_gasto['custo_formatado']}"
                                )
                            else:
                                st.metric("Frota com Maior Gasto", "N/A")
                        with col_resumo3:
                            st.metric(
                                "Classe com Maior Gasto", 
                                f"{gastos_por_classe.iloc[0]['Classe_Operacional'] if not gastos_por_classe.empty else 'N/A'}"
                            )
                    else:
                        st.warning("Cadastre os preços de combustível na aba Importar > Preços para visualizar os gastos.")

                    st.markdown("---")
                    st.subheader("📈 Média de Consumo por Classe Operacional")
                    df_media = df_f[(df_f['Media'].notna()) & (df_f['Media'] > 0)].copy()

                    classes_para_excluir = ['MOTOCICLETA', 'VEICULOS LEVES', 'USINA', 'MINI CARREGADEIRA']

                    # Verificar se a coluna Classe_Operacional existe antes de filtrar
                    if 'Classe_Operacional' in df_media.columns:
                        df_media_filtrado = df_media[~df_media['Classe_Operacional'].str.upper().isin(classes_para_excluir)]
                    else:
                        df_media_filtrado = df_media

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
                    
                    st.markdown("---")
                     
                    st.subheader("🏆 Ranking de Eficiência Inteligente")
                    
                    # Explicação do ranking
                    with st.expander("ℹ️ Como interpretar o Ranking de Eficiência"):
                        st.markdown("""
                        **📊 Como funciona:**
                        - **🟢 Verde (+5%+):** Equipamento mais eficiente que a média da sua classe
                        - **⚪ Branco (-5% a +5%):** Eficiência próxima à média da classe  
                        - **🔴 Vermelho (-5%-):** Equipamento menos eficiente que a média da sua classe
                        
                        **🎯 Objetivo:** Identificar equipamentos que consomem menos combustível por hora/km comparado aos similares.
                        """)
                    
                    if 'Media' in df.columns and not df['Media'].dropna().empty:
                        media_por_classe = df.groupby('Classe_Operacional')['Media'].mean().to_dict()
                        ranking_df = df.copy()
                        ranking_df['Media_Classe'] = ranking_df['Classe_Operacional'].map(media_por_classe)
                        ranking_df['Eficiencia_%'] = ((ranking_df['Media_Classe'] / ranking_df['Media']) - 1) * 100
                        
                        ranking = ranking_df.groupby(['Cod_Equip', 'DESCRICAO_EQUIPAMENTO', 'Classe_Operacional'])['Eficiencia_%'].mean().sort_values(ascending=False).reset_index()
                        ranking.rename(columns={'DESCRICAO_EQUIPAMENTO': 'Equipamento', 'Eficiencia_%': 'Eficiência (%)'}, inplace=True)
                        
                        # Adicionar informações da frota
                        ranking = ranking.merge(
                            df_frotas[['Cod_Equip', 'PLACA', 'ATIVO']], 
                            on='Cod_Equip', 
                            how='left'
                        )
                        
                        # Criar coluna combinada mais informativa
                        ranking['Equipamento_Completo'] = ranking.apply(
                            lambda row: f"{row['Cod_Equip']} - {row['Equipamento'][:25]}{'...' if len(str(row['Equipamento'])) > 25 else ''} ({row['PLACA']})", 
                            axis=1
                        )
                        
                        # Melhorar formatação da eficiência
                        def formatar_eficiencia_melhorada(val):
                            if pd.isna(val): return "N/A"
                            if val > 10: return f"🟢 Excelente (+{val:+.1f}%)".replace('.',',')
                            elif val > 5: return f"🟢 Bom (+{val:+.1f}%)".replace('.',',')
                            elif val > 0: return f"🟢 Acima (+{val:+.1f}%)".replace('.',',')
                            elif val > -5: return f"⚪ Média ({val:+.1f}%)".replace('.',',')
                            elif val > -10: return f"🟡 Abaixo ({val:+.1f}%)".replace('.',',')
                            else: return f"🔴 Crítico ({val:+.1f}%)".replace('.',',')
                        
                        ranking['Eficiência_Formatada'] = ranking['Eficiência (%)'].apply(formatar_eficiencia_melhorada)
                        
                        # Adicionar status do equipamento
                        ranking['Status'] = ranking['ATIVO'].apply(lambda x: "✅ Ativo" if x == 'ATIVO' else "❌ Inativo")
                        
                        # Mostrar estatísticas rápidas
                        col1, col2, col3, col4 = st.columns(4)
                        with col1:
                            excelentes = len(ranking[ranking['Eficiência (%)'] > 10])
                            st.metric("🟢 Excelentes", excelentes)
                        with col2:
                            bons = len(ranking[(ranking['Eficiência (%)'] > 5) & (ranking['Eficiência (%)'] <= 10)])
                            st.metric("🟢 Bons", bons)
                        with col3:
                            criticos = len(ranking[ranking['Eficiência (%)'] < -10])
                            st.metric("🔴 Críticos", criticos)
                        with col4:
                            total_analisados = len(ranking)
                            st.metric("📊 Total", total_analisados)
                        
                        # Filtros para o ranking
                        st.markdown("**🔍 Filtros:**")
                        col_filtro1, col_filtro2, col_filtro3 = st.columns(3)
                        
                        with col_filtro1:
                            mostrar_inativos = st.checkbox("Mostrar Inativos", value=False)
                        with col_filtro2:
                            classe_filtro = st.selectbox(
                                "Filtrar por Classe",
                                ["Todas"] + sorted(ranking['Classe_Operacional'].unique().tolist())
                            )
                        with col_filtro3:
                            eficiencia_filtro = st.selectbox(
                                "Filtrar por Eficiência",
                                ["Todas", "Excelentes (+10%+)", "Bons (+5%+)", "Acima da Média", "Média", "Abaixo da Média", "Críticos (-10%-)"]
                            )
                        
                        # Aplicar filtros
                        ranking_filtrado = ranking.copy()
                        
                        if not mostrar_inativos:
                            ranking_filtrado = ranking_filtrado[ranking_filtrado['ATIVO'] == 'ATIVO']
                        
                        if classe_filtro != "Todas":
                            ranking_filtrado = ranking_filtrado[ranking_filtrado['Classe_Operacional'] == classe_filtro]
                        
                        if eficiencia_filtro != "Todas":
                            if eficiencia_filtro == "Excelentes (+10%+)":
                                ranking_filtrado = ranking_filtrado[ranking_filtrado['Eficiência (%)'] > 10]
                            elif eficiencia_filtro == "Bons (+5%+)":
                                ranking_filtrado = ranking_filtrado[ranking_filtrado['Eficiência (%)'] > 5]
                            elif eficiencia_filtro == "Acima da Média":
                                ranking_filtrado = ranking_filtrado[ranking_filtrado['Eficiência (%)'] > 0]
                            elif eficiencia_filtro == "Média":
                                ranking_filtrado = ranking_filtrado[(ranking_filtrado['Eficiência (%)'] >= -5) & (ranking_filtrado['Eficiência (%)'] <= 5)]
                            elif eficiencia_filtro == "Abaixo da Média":
                                ranking_filtrado = ranking_filtrado[ranking_filtrado['Eficiência (%)'] < 0]
                            elif eficiencia_filtro == "Críticos (-10%-)":
                                ranking_filtrado = ranking_filtrado[ranking_filtrado['Eficiência (%)'] < -10]
                        
                        # Exibir ranking com informações melhoradas
                        if not ranking_filtrado.empty:
                            # Criar gráfico de barras para visualização
                            fig_ranking = px.bar(
                                ranking_filtrado.head(20),
                                x='Eficiência (%)',
                                y='Equipamento_Completo',
                                orientation='h',
                                color='Eficiência (%)',
                                color_continuous_scale='RdYlGn',
                                title="Top 20 Equipamentos por Eficiência",
                                labels={'Eficiência (%)': 'Eficiência vs Média da Classe (%)', 'Equipamento_Completo': 'Equipamento'}
                            )
                            fig_ranking.update_layout(
                                yaxis={'categoryorder':'total ascending'},
                                height=600,
                                xaxis_title="Eficiência vs Média da Classe (%)",
                                yaxis_title="Equipamento"
                            )
                            st.plotly_chart(fig_ranking, use_container_width=True)
                            
                            # Tabela detalhada
                            st.markdown("**📋 Tabela Detalhada:**")
                            colunas_exibir = ['Equipamento_Completo', 'Classe_Operacional', 'Eficiência_Formatada', 'Status']
                            st.dataframe(
                                ranking_filtrado[colunas_exibir].head(50),
                                use_container_width=True,
                                hide_index=True
                            )
                            
                            # Botão de exportação
                            csv_ranking = para_csv(ranking_filtrado)
                            st.download_button(
                                "📥 Exportar Ranking Filtrado para CSV", 
                                csv_ranking, 
                                "ranking_eficiencia_filtrado.csv", 
                                "text/csv"
                            )
                        else:
                            st.warning("Nenhum equipamento encontrado com os filtros selecionados.")
                    else:
                        st.info("Não há dados de consumo médio para gerar o ranking.")
                        
                st.markdown("---")
                st.subheader("📈 Análise de Tendências e Evolução")
                
                # ===== SEÇÃO: TENDÊNCIA DE CONSUMO MENSAAL MELHORADA =====
                st.markdown("**📊 Tendência de Consumo Mensal**")
                
                if not df.empty and 'Qtde_Litros' in df.columns:
                    # Agrupa os dados por Ano/Mês e soma o consumo
                    consumo_mensal = df.groupby('AnoMes')['Qtde_Litros'].sum().reset_index().sort_values('AnoMes')
                    
                    if not consumo_mensal.empty:
                        # Calcular estatísticas da tendência
                        consumo_atual = consumo_mensal['Qtde_Litros'].iloc[-1] if len(consumo_mensal) > 0 else 0
                        consumo_anterior = consumo_mensal['Qtde_Litros'].iloc[-2] if len(consumo_mensal) > 1 else 0
                        variacao = ((consumo_atual - consumo_anterior) / consumo_anterior * 100) if consumo_anterior > 0 else 0
                        
                        # Mostrar métricas de tendência
                        col_tend1, col_tend2, col_tend3 = st.columns(3)
                        with col_tend1:
                            st.metric("📈 Consumo Atual", f"{formatar_brasileiro_int(consumo_atual)} L")
                        with col_tend2:
                            st.metric("📊 Consumo Anterior", f"{formatar_brasileiro_int(consumo_anterior)} L")
                        with col_tend3:
                            st.metric("🔄 Variação", f"{variacao:+.1f}%".replace('.',','))
                        
                        # Gráfico de tendência melhorado
                        fig_tendencia = px.line(
                            consumo_mensal,
                            x='AnoMes',
                            y='Qtde_Litros',
                            title="📈 Evolução do Consumo de Combustível",
                            labels={"AnoMes": "Mês/Ano", "Qtde_Litros": "Litros Consumidos"},
                            markers=True,
                            line_shape='linear'
                        )
                        fig_tendencia.update_layout(
                            xaxis_title="Mês/Ano", 
                            yaxis_title="Litros Consumidos",
                            height=400,
                            showlegend=False
                        )
                        fig_tendencia.update_traces(
                            line=dict(width=3, color='#1f77b4'),
                            marker=dict(size=8, color='#1f77b4')
                        )
                        st.plotly_chart(fig_tendencia, use_container_width=True)
                        
                        # Análise de sazonalidade
                        if len(consumo_mensal) >= 12:  # Pelo menos 1 ano de dados
                            st.markdown("**📅 Análise de Sazonalidade:**")
                            # Calcular média por mês
                            consumo_mensal['Mes'] = consumo_mensal['AnoMes'].str[-2:] if 'AnoMes' in consumo_mensal.columns else '01'
                            sazonalidade = consumo_mensal.groupby('Mes')['Qtde_Litros'].mean().reset_index()
                            sazonalidade['Mes_Nome'] = sazonalidade['Mes'].map({
                                '01': 'Jan', '02': 'Fev', '03': 'Mar', '04': 'Abr',
                                '05': 'Mai', '06': 'Jun', '07': 'Jul', '08': 'Ago',
                                '09': 'Set', '10': 'Out', '11': 'Nov', '12': 'Dez'
                            })
                            
                            fig_sazonalidade = px.bar(
                                sazonalidade,
                                x='Mes_Nome',
                                y='Qtde_Litros',
                                title="📅 Consumo Médio por Mês (Sazonalidade)",
                                labels={'Mes_Nome': 'Mês', 'Qtde_Litros': 'Litros Médios'},
                                color='Qtde_Litros',
                                color_continuous_scale='Blues'
                            )
                            fig_sazonalidade.update_layout(height=300)
                            st.plotly_chart(fig_sazonalidade, use_container_width=True)
                    else:
                        st.info("Não há dados suficientes para gerar o gráfico de tendência com os filtros selecionados.")
                
                # ===== NOVA SEÇÃO: ANÁLISE DE EFICIÊNCIA TEMPORAL =====
                st.markdown("---")
                st.markdown("**🎯 Análise de Eficiência Temporal**")
                
                if 'Media' in df.columns and not df['Media'].dropna().empty:
                    # Análise de eficiência ao longo do tempo
                    df_eficiencia_tempo = df[df['Media'].notna()].copy()
                    df_eficiencia_tempo['AnoMes'] = df_eficiencia_tempo['AnoMes'] if 'AnoMes' in df_eficiencia_tempo.columns else '2024-01'
                    
                    eficiencia_temporal = df_eficiencia_tempo.groupby('AnoMes')['Media'].mean().reset_index()
                    
                    if not eficiencia_temporal.empty:
                        fig_eficiencia_tempo = px.line(
                            eficiencia_temporal,
                            x='AnoMes',
                            y='Media',
                            title="📊 Evolução da Eficiência Média da Frota",
                            labels={"AnoMes": "Mês/Ano", "Media": "Eficiência Média (L/h ou km/L)"},
                            markers=True
                        )
                        fig_eficiencia_tempo.update_layout(
                            xaxis_title="Mês/Ano", 
                            yaxis_title="Eficiência Média",
                            height=300
                        )
                        st.plotly_chart(fig_eficiencia_tempo, use_container_width=True)
                        
                        # Estatísticas de eficiência
                        eficiencia_atual = eficiencia_temporal['Media'].iloc[-1] if len(eficiencia_temporal) > 0 else 0
                        eficiencia_anterior = eficiencia_temporal['Media'].iloc[-2] if len(eficiencia_temporal) > 1 else 0
                        variacao_eficiencia = ((eficiencia_atual - eficiencia_anterior) / eficiencia_anterior * 100) if eficiencia_anterior > 0 else 0
                        
                        col_ef1, col_ef2, col_ef3 = st.columns(3)
                        with col_ef1:
                            st.metric("🎯 Eficiência Atual", f"{formatar_brasileiro(eficiencia_atual)}")
                        with col_ef2:
                            st.metric("📊 Eficiência Anterior", f"{formatar_brasileiro(eficiencia_anterior)}")
                        with col_ef3:
                            st.metric("🔄 Variação", f"{variacao_eficiencia:+.1f}%".replace('.',','))
                    else:
                        st.info("Não há dados suficientes para análise de eficiência temporal.")
                else:
                    st.info("Não há dados de eficiência para análise temporal.")

                    st.markdown("---")
                    st.subheader("💰 Total de Gasto por Motorista")
                    precos_map = get_precos_combustivel_map()
                    if precos_map:
                        # Vincula combustível por frota e multiplica litros por preço
                        df_tmp = df_f.copy()
                        
                        # Verificar se a coluna tipo_combustivel existe em df_frotas
                        if 'tipo_combustivel' in df_frotas.columns:
                            df_tmp = df_tmp.merge(df_frotas[['Cod_Equip','tipo_combustivel']], on='Cod_Equip', how='left')
                            # Verificar se a coluna foi criada após o merge
                            if 'tipo_combustivel' in df_tmp.columns:
                                df_tmp['tipo_combustivel'] = df_tmp['tipo_combustivel'].fillna('Diesel S500')
                            else:
                                df_tmp['tipo_combustivel'] = 'Diesel S500'
                        else:
                            # Se não existir, criar a coluna com valor padrão
                            df_tmp['tipo_combustivel'] = 'Diesel S500'
                        # Garantir que a coluna tipo_combustivel existe antes de mapear preços
                        if 'tipo_combustivel' not in df_tmp.columns:
                            df_tmp['tipo_combustivel'] = 'Diesel S500'
                        
                        df_tmp['preco_unit'] = df_tmp['tipo_combustivel'].map(precos_map).fillna(0.0)
                        df_tmp['custo'] = df_tmp['Qtde_Litros'].fillna(0.0) * df_tmp['preco_unit']
                        # Agrupar por matrícula
                        if 'Matricula' in df_tmp.columns:
                            gasto_motorista = df_tmp.groupby('Matricula').agg({'custo':'sum', 'Qtde_Litros':'sum'}).sort_values('custo', ascending=False)
                            gasto_motorista = gasto_motorista[gasto_motorista['custo']>0]
                            if not gasto_motorista.empty:
                                gasto_motorista = gasto_motorista.reset_index()
                                gasto_motorista['Custo (R$)'] = gasto_motorista['custo'].apply(lambda x: formatar_brasileiro(x, 'R$ '))
                                gasto_motorista['Litros'] = gasto_motorista['Qtde_Litros'].apply(formatar_brasileiro_int)
                                st.dataframe(gasto_motorista[['Matricula','Litros','Custo (R$)']])
                                try:
                                    fig_gasto = px.bar(gasto_motorista.head(10), x='custo', y='Matricula', orientation='h', text='Custo (R$)', labels={'custo':'Custo (R$)','Matricula':'Matrícula'})
                                    st.plotly_chart(fig_gasto, use_container_width=True)
                                except Exception:
                                    pass
                            else:
                                st.info("Sem dados suficientes de custo (verifique preços cadastrados).")
                        else:
                            st.info("Não há coluna de matrícula nos abastecimentos para calcular o gasto por motorista.")
                    else:
                        st.info("Cadastre os preços de combustível na aba Importar > Preços.")

                    st.markdown("---")
                    st.subheader("🔄 Análise de Proporções por Classe e Combustível")
                
                # Criar DataFrame com informações de combustível
                df_consumo_combustivel = df_f.copy()
                
                # Verificar se a coluna tipo_combustivel existe em df_frotas
                if 'tipo_combustivel' in df_frotas.columns:
                    try:
                        frotas_combustivel = df_frotas[['Cod_Equip', 'tipo_combustivel']].copy()
                        frotas_combustivel['tipo_combustivel'] = frotas_combustivel['tipo_combustivel'].fillna('Diesel S500')
                        df_consumo_combustivel = df_consumo_combustivel.merge(
                            frotas_combustivel, 
                            on='Cod_Equip', 
                            how='left'
                        )
                        # Verificar se a coluna foi criada após o merge
                        if 'tipo_combustivel' not in df_consumo_combustivel.columns:
                            df_consumo_combustivel['tipo_combustivel'] = 'Diesel S500'
                    except Exception:
                        df_consumo_combustivel['tipo_combustivel'] = 'Diesel S500'
                else:
                    df_consumo_combustivel['tipo_combustivel'] = 'Diesel S500'
                
                # Garantir que a coluna tipo_combustivel existe
                if 'tipo_combustivel' not in df_consumo_combustivel.columns:
                    df_consumo_combustivel['tipo_combustivel'] = 'Diesel S500'
                
                col_grafico1, col_grafico2 = st.columns(2)
                
                with col_grafico1:
                    st.subheader("📊 Consumo por Classe (Visão Macro)")
                    # Excluir "Usina" e frotas sem classe, usar Classe_Operacional
                    classes_a_excluir_macro = ['USINA', 'USINA MOBILE', 'USINA FIXA']
                    # Verificar se a coluna Classe_Operacional existe antes de filtrar
                    if 'Classe_Operacional' in df_consumo_combustivel.columns:
                        df_consumo_classe_macro = df_consumo_combustivel[
                            (df_consumo_combustivel['Classe_Operacional'].notna()) & 
                            (~df_consumo_combustivel['Classe_Operacional'].str.upper().isin(classes_a_excluir_macro))
                        ]
                    else:
                        df_consumo_classe_macro = df_consumo_combustivel
                    
                    if not df_consumo_classe_macro.empty:
                        try:
                            consumo_por_classe_macro = df_consumo_classe_macro.groupby("Classe_Operacional")["Qtde_Litros"].sum().sort_values(ascending=False).reset_index()
                            
                            # Criar gráfico de pizza
                            fig_pizza_classe = px.pie(
                                consumo_por_classe_macro, 
                                values='Qtde_Litros', 
                                names='Classe_Operacional',
                                title="Proporção de Consumo por Classe",
                                hole=0.3
                            )
                            fig_pizza_classe.update_traces(textposition='inside', textinfo='percent+label')
                            fig_pizza_classe.update_layout(height=400)
                            st.plotly_chart(fig_pizza_classe, use_container_width=True)
                            
                            # Mostrar totais
                            st.info(f"**Total de classes analisadas:** {len(consumo_por_classe_macro)}")
                            st.info(f"**Total de litros consumidos:** {formatar_brasileiro_int(consumo_por_classe_macro['Qtde_Litros'].sum())} L")
                        except Exception as e:
                            st.error(f"Erro ao criar gráfico de classe: {e}")
                    else:
                        st.warning("Não há dados suficientes para análise por classe.")
                
                with col_grafico2:
                    st.subheader("⛽ Consumo por Tipo de Combustível")
                    if not df_consumo_combustivel.empty and 'tipo_combustivel' in df_consumo_combustivel.columns:
                        try:
                            consumo_por_combustivel = df_consumo_combustivel.groupby("tipo_combustivel")["Qtde_Litros"].sum().sort_values(ascending=False).reset_index()
                            
                            # Criar gráfico de pizza
                            fig_pizza_combustivel = px.pie(
                                consumo_por_combustivel, 
                                values='Qtde_Litros', 
                                names='tipo_combustivel',
                                title="Proporção de Consumo por Combustível",
                                hole=0.3
                            )
                            fig_pizza_combustivel.update_traces(textposition='inside', textinfo='percent+label')
                            fig_pizza_combustivel.update_layout(height=400)
                            st.plotly_chart(fig_pizza_combustivel, use_container_width=True)
                            
                            # Mostrar totais
                            st.info(f"**Total de tipos de combustível:** {len(consumo_por_combustivel)}")
                            st.info(f"**Total de litros consumidos:** {formatar_brasileiro_int(consumo_por_combustivel['Qtde_Litros'].sum())} L")
                        except Exception:
                            df_consumo_combustivel['tipo_combustivel'] = 'Diesel S500'
                            consumo_por_combustivel = df_consumo_combustivel.groupby("tipo_combustivel")["Qtde_Litros"].sum().reset_index()
                            st.info(f"**Total de litros consumidos:** {formatar_brasileiro_int(consumo_por_combustivel['Qtde_Litros'].sum())} L")
                    else:
                        st.warning("Não há dados suficientes para análise por combustível.")
                        

                    st.markdown("---")
                    st.subheader("📊 Demonstrativos Detalhados dos Pneus")

                    df_pneus_all = get_pneus_historico()
                    if not df_pneus_all.empty:
                        # Adicione colunas de status e vida se não existirem
                        if 'status' not in df_pneus_all.columns:
                            df_pneus_all['status'] = 'Ativo'
                        if 'vida_atual' not in df_pneus_all.columns:
                            df_pneus_all['vida_atual'] = 1

                        total_pneus = len(df_pneus_all)
                        ativos = df_pneus_all[df_pneus_all['status'].str.lower() == 'ativo'].shape[0]
                        sucateados = df_pneus_all[df_pneus_all['status'].str.lower() == 'sucateado'].shape[0]
                        reformados = df_pneus_all[df_pneus_all['status'].str.lower() == 'reformado'].shape[0]
                        vidas = df_pneus_all['vida_atual'].value_counts().sort_index()
                        marcas = df_pneus_all['marca'].value_counts()
                        modelos = df_pneus_all['modelo'].value_counts()
                        posicoes = df_pneus_all['posicao'].value_counts()

                        col1, col2, col3, col4 = st.columns(4)
                        col1.metric("Total de Pneus", total_pneus)
                        col2.metric("Ativos", ativos)
                        col3.metric("Sucateados", sucateados)
                        col4.metric("Reformados", reformados)

                        st.markdown("#### Distribuição por Vida Atual")
                        vidas_df = vidas.reset_index()
                        vidas_df.columns = ["Vida", "Quantidade"]
                        st.dataframe(vidas_df)

                        st.markdown("#### Distribuição por Status")
                        status_df = df_pneus_all['status'].value_counts().reset_index()
                        status_df.columns = ["Status", "Quantidade"]
                        st.dataframe(status_df)

                        st.markdown("#### Distribuição por Marca")
                        st.dataframe(marcas.reset_index().rename(columns={'index': 'Marca', 'marca': 'Quantidade'}))

                        st.markdown("#### Distribuição por Modelo")
                        st.dataframe(modelos.reset_index().rename(columns={'index': 'Modelo', 'modelo': 'Quantidade'}))

                        st.markdown("#### Distribuição por Posição")
                        st.dataframe(posicoes.reset_index().rename(columns={'index': 'Posição', 'posicao': 'Quantidade'}))

                        # Gráficos
                        fig_status = px.pie(status_df, names='Status', values='Quantidade', title='Status dos Pneus')
                        st.plotly_chart(fig_status, use_container_width=True)

                        fig_vidas = px.bar(vidas_df, x='Vida', y='Quantidade', title='Quantidade de Pneus por Vida')
                        st.plotly_chart(fig_vidas, use_container_width=True)

                        fig_marcas = px.bar(marcas.reset_index(), x='index', y='marca', title='Quantidade por Marca')
                        st.plotly_chart(fig_marcas, use_container_width=True)

                        fig_modelos = px.bar(modelos.reset_index(), x='index', y='modelo', title='Quantidade por Modelo')
                        st.plotly_chart(fig_modelos, use_container_width=True)

                        fig_posicoes = px.bar(posicoes.reset_index(), x='index', y='posicao', title='Quantidade por Posição')
                        st.plotly_chart(fig_posicoes, use_container_width=True)

                    else:
                        st.info("Nenhum pneu cadastrado para demonstrativo.")

                    st.markdown("---")
                    st.subheader("🛢️ Demonstrativos de Lubrificantes")

                    ensure_lubrificantes_schema()
                    conn = sqlite3.connect(DB_PATH)
                    df_lub = pd.read_sql("SELECT * FROM lubrificantes", conn)
                    df_mov = pd.read_sql("SELECT * FROM lubrificantes_movimentacoes", conn)

                    st.write("**Estoque Atual de Lubrificantes:**")
                    if not df_lub.empty:
                            # Separar por tipo
                            df_oleos = df_lub[df_lub['tipo'].str.lower() == 'óleo']
                            df_graxas = df_lub[df_lub['tipo'].str.lower() == 'graxa']

                            col_o, col_g = st.columns(2)
                            with col_o:
                                st.markdown("#### Estoque de Óleos")
                                if not df_oleos.empty:
                                    fig_oleos = px.bar(
                                        df_oleos,
                                        x='nome',
                                        y='quantidade_estoque',
                                        color='viscosidade',
                                        text='quantidade_estoque',
                                        title="Óleos - Estoque Atual",
                                        labels={'quantidade_estoque': 'Qtd. Estoque', 'nome': 'Óleo'}
                                    )
                                    st.plotly_chart(fig_oleos, use_container_width=True)
                                else:
                                    st.info("Nenhum óleo cadastrado.")

                            with col_g:
                                st.markdown("#### Estoque de Graxas")
                                if not df_graxas.empty:
                                    fig_graxas = px.bar(
                                        df_graxas,
                                        x='nome',
                                        y='quantidade_estoque',
                                        color='viscosidade',
                                        text='quantidade_estoque',
                                        title="Graxas - Estoque Atual",
                                        labels={'quantidade_estoque': 'Qtd. Estoque', 'nome': 'Graxa'}
                                    )
                                    st.plotly_chart(fig_graxas, use_container_width=True)
                                else:
                                    st.info("Nenhuma graxa cadastrada.")

                            # Pizza geral
                            df_lub['tipo'] = df_lub['tipo'].fillna('óleo')
                            fig_pizza = px.pie(
                                df_lub,
                                names='tipo',
                                values='quantidade_estoque',
                                title="Proporção de Estoque: Óleos vs Graxas"
                            )
                            st.plotly_chart(fig_pizza, use_container_width=True)

                            st.write("**Movimentações Recentes:**")
                            df_mov['data'] = pd.to_datetime(df_mov['data'], errors='coerce')
                            df_mov = df_mov.sort_values('data', ascending=False)
                            st.dataframe(df_mov.head(20))
                    else:
                            st.info("Nenhum lubrificante cadastrado.")

                    conn.close()
            
        if tab_consulta is not None:
            with tab_consulta:
                st.header("🔎 Ficha Individual do Equipamento")
                # Permitir consulta direta por código (Cód Equipamento)
                cod_input = st.text_input("Digite o código da frota")
                if cod_input and cod_input.isdigit() and int(cod_input) in df_frotas['Cod_Equip'].values:
                    equip_label = df_frotas.loc[df_frotas['Cod_Equip'] == int(cod_input)].iloc[0]['label']
                else:
                    equip_label = None
            
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

                    # Análise do motorista com uso mais frequente
                    st.markdown("---")
                    st.subheader("👤 Análise de Uso por Motorista")
                    
                    if not consumo_eq.empty and 'Matricula' in consumo_eq.columns:
                        # Análise por motorista (matrícula)
                        uso_por_motorista = consumo_eq.groupby('Matricula').agg({
                            'Qtde_Litros': 'sum',
                            'Data': 'count'
                        }).rename(columns={'Data': 'Abastecimentos'}).sort_values('Qtde_Litros', ascending=False)
                        
                        if not uso_por_motorista.empty:
                            # Top 5 motoristas com maior consumo
                            top_motoristas = uso_por_motorista.head(5).reset_index()
                            top_motoristas['Consumo (L)'] = top_motoristas['Qtde_Litros'].apply(formatar_brasileiro_int)
                            top_motoristas['Abastecimentos'] = top_motoristas['Abastecimentos'].astype(int)
                            
                            col_motorista1, col_motorista2 = st.columns(2)
                            
                            with col_motorista1:
                                st.subheader("🏆 Top 5 Motoristas por Consumo")
                                st.dataframe(
                                    top_motoristas[['Matricula', 'Consumo (L)', 'Abastecimentos']], 
                                    use_container_width=True
                                )
                            
                            with col_motorista2:
                                st.subheader("📊 Motorista com Maior Uso")
                                motorista_mais_frequente = top_motoristas.iloc[0]
                                st.metric(
                                    "Motorista Principal", 
                                    f"Matrícula {motorista_mais_frequente['Matricula']}",
                                    f"{motorista_mais_frequente['Consumo (L)']} L"
                                )
                                st.metric(
                                    "Total de Abastecimentos", 
                                    motorista_mais_frequente['Abastecimentos']
                                )
                                st.metric(
                                    "Percentual do Total", 
                                    f"{(motorista_mais_frequente['Qtde_Litros'] / uso_por_motorista['Qtde_Litros'].sum() * 100):.1f}%"
                                )
                            
                            # Gráfico de consumo por motorista
                            st.subheader("📈 Consumo por Motorista")
                            fig_motoristas = px.bar(
                                top_motoristas,
                                x='Qtde_Litros',
                                y='Matricula',
                                orientation='h',
                                text='Consumo (L)',
                                title="Consumo de Combustível por Motorista",
                                labels={'Qtde_Litros': 'Litros Consumidos', 'Matricula': 'Matrícula'}
                            )
                            fig_motoristas.update_traces(
                                textposition='outside',
                                marker_color='#2ca02c'
                            )
                            fig_motoristas.update_layout(
                                yaxis={'categoryorder':'total ascending'},
                                height=400
                            )
                            st.plotly_chart(fig_motoristas, use_container_width=True)
                            
                        else:
                            st.info("Não há dados de motoristas (matrículas) para este equipamento.")
                    else:
                        st.info("Não há dados de consumo ou coluna de matrícula para análise de motoristas.")

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

                    # NOVA SEÇÃO: Gastos com Combustível por Frota vs Classe
                    st.subheader("💰 Gastos com Combustível")
                    
                    precos_map = get_precos_combustivel_map()
                    if precos_map:
                        # Calcular gasto da frota selecionada
                        df_frota_gastos = consumo_eq.copy()
                        # Verificar se a coluna tipo_combustivel existe em df_frotas
                        if 'tipo_combustivel' in df_frotas.columns:
                            df_frota_gastos = df_frota_gastos.merge(
                                df_frotas[['Cod_Equip', 'tipo_combustivel']], 
                                on='Cod_Equip', 
                                how='left'
                            )
                            # Verificar se a coluna foi criada após o merge
                            if 'tipo_combustivel' in df_frota_gastos.columns:
                                df_frota_gastos['tipo_combustivel'] = df_frota_gastos['tipo_combustivel'].fillna('Diesel S500')
                            else:
                                df_frota_gastos['tipo_combustivel'] = 'Diesel S500'
                        else:
                            # Se não existir, criar a coluna com valor padrão
                            df_frota_gastos['tipo_combustivel'] = 'Diesel S500'
                        df_frota_gastos['preco_unit'] = df_frota_gastos['tipo_combustivel'].map(precos_map).fillna(0.0)
                        df_frota_gastos['custo'] = df_frota_gastos['Qtde_Litros'].fillna(0.0) * df_frota_gastos['preco_unit']
                        
                        gasto_frota = df_frota_gastos['custo'].sum()
                        
                        # Calcular gasto total da classe
                        classe_selecionada = dados_eq.get('Classe_Operacional')
                        gasto_classe_total = 0
                        if classe_selecionada:
                            df_classe_gastos = df[df['Classe_Operacional'] == classe_selecionada].copy()
                            # Verificar se a coluna tipo_combustivel existe em df_frotas
                            if 'tipo_combustivel' in df_frotas.columns:
                                df_classe_gastos = df_classe_gastos.merge(
                                    df_frotas[['Cod_Equip', 'tipo_combustivel']], 
                                    on='Cod_Equip', 
                                    how='left'
                                )
                                # Verificar se a coluna foi criada após o merge
                                if 'tipo_combustivel' in df_classe_gastos.columns:
                                    df_classe_gastos['tipo_combustivel'] = df_classe_gastos['tipo_combustivel'].fillna('Diesel S500')
                                else:
                                    df_classe_gastos['tipo_combustivel'] = 'Diesel S500'
                            else:
                                # Se não existir, criar a coluna com valor padrão
                                df_classe_gastos['tipo_combustivel'] = 'Diesel S500'
                            
                            df_classe_gastos['preco_unit'] = df_classe_gastos['tipo_combustivel'].map(precos_map).fillna(0.0)
                            df_classe_gastos['custo'] = df_classe_gastos['Qtde_Litros'].fillna(0.0) * df_classe_gastos['preco_unit']
                            gasto_classe_total = df_classe_gastos['custo'].sum()
                        
                        # Calcular porcentagem
                        porcentagem_classe = (gasto_frota / gasto_classe_total * 100) if gasto_classe_total > 0 else 0
                        
                        # Exibir métricas
                        col_gasto1, col_gasto2, col_gasto3 = st.columns(3)
                        
                        with col_gasto1:
                            st.metric(
                                "💰 Gasto da Frota", 
                                formatar_brasileiro(gasto_frota, 'R$ '),
                                help="Total gasto com combustível por esta frota"
                            )
                        
                        with col_gasto2:
                            st.metric(
                                "💰 Gasto Total da Classe", 
                                formatar_brasileiro(gasto_classe_total, 'R$ '),
                                help="Total gasto com combustível por todas as frotas da mesma classe"
                            )
                        
                        with col_gasto3:
                            st.metric(
                                "📊 % da Classe", 
                                f"{porcentagem_classe:.1f}%",
                                help="Porcentagem que esta frota representa do gasto total da classe"
                            )
                        
                        # Gráfico de comparação
                        if gasto_classe_total > 0:
                            df_comparacao_gastos = pd.DataFrame({
                                'Categoria': ['Esta Frota', 'Outras Frotas da Classe'],
                                'Gasto (R$)': [gasto_frota, gasto_classe_total - gasto_frota]
                            })
                            
                            fig_gastos = px.pie(
                                df_comparacao_gastos,
                                values='Gasto (R$)',
                                names='Categoria',
                                title=f"Distribuição de Gastos na Classe {classe_selecionada}",
                                color_discrete_map={
                                    'Esta Frota': '#ff7f0e',
                                    'Outras Frotas da Classe': '#1f77b4'
                                }
                            )
                            fig_gastos.update_traces(
                                textposition='inside',
                                textinfo='percent+label',
                                textfont_size=14
                            )
                            fig_gastos.update_layout(height=400)
                            st.plotly_chart(fig_gastos, use_container_width=True)
                            
                            # Informações adicionais
                            st.info(f"""
                            **📊 Resumo dos Gastos:**
                            - Esta frota representa **{porcentagem_classe:.1f}%** do gasto total da classe **{classe_selecionada}**
                            - Gasto médio por frota na classe: **{formatar_brasileiro(gasto_classe_total / df_frotas[df_frotas['Classe_Operacional'] == classe_selecionada].shape[0], 'R$ ')}**
                            """)
                    else:
                        st.warning("⚠️ Para visualizar os gastos com combustível, configure os preços na aba 'Importar Dados > Preços de Combustível'.")
                    
                    st.markdown("---")
                    st.subheader("⛽ Consumo Total da Frota")

                    if not consumo_eq.empty:
                        # Calcular consumo total em litros
                        consumo_total_litros = consumo_eq['Qtde_Litros'].sum()

                        # Calcular consumo por período (últimos 30, 90, 365 dias)
                        hoje = pd.Timestamp.now()
                        periodos = {
                            'Últimos 30 dias': 30,
                            'Últimos 90 dias': 90,
                            'Últimos 365 dias': 365
                        }

                        consumos_periodo = {}
                        for nome_periodo, dias in periodos.items():
                            data_limite = hoje - pd.Timedelta(days=dias)
                            consumo_periodo = consumo_eq[consumo_eq['Data'] >= data_limite]['Qtde_Litros'].sum()
                            consumos_periodo[nome_periodo] = consumo_periodo

                        # Calcular consumo da classe para comparação
                        classe_selecionada = dados_eq.get('Classe_Operacional')
                        consumo_classe_total = 0
                        if classe_selecionada:
                            df_classe_consumo = df[df['Classe_Operacional'] == classe_selecionada]
                            consumo_classe_total = df_classe_consumo['Qtde_Litros'].sum()

                        # Calcular porcentagem do consumo da classe
                        porcentagem_consumo_classe = (consumo_total_litros / consumo_classe_total * 100) if consumo_classe_total > 0 else 0

                        # Métricas de consumo
                        col_consumo1, col_consumo2, col_consumo3, col_consumo4, col_consumo5 = st.columns(5)

                        with col_consumo1:
                            st.metric(
                                "📊 Consumo Total",
                                f"{formatar_brasileiro_int(consumo_total_litros)} L",
                                help="Total de litros consumidos por esta frota"
                            )

                        with col_consumo2:
                            st.metric(
                                "📅 Últimos 30 dias",
                                f"{formatar_brasileiro_int(consumos_periodo['Últimos 30 dias'])} L"
                            )

                        with col_consumo3:
                            st.metric(
                                "📅 Últimos 90 dias",
                                f"{formatar_brasileiro_int(consumos_periodo['Últimos 90 dias'])} L"
                            )

                        with col_consumo4:
                            st.metric(
                                "📅 Últimos 365 dias",
                                f"{formatar_brasileiro_int(consumos_periodo['Últimos 365 dias'])} L"
                            )

                        with col_consumo5:
                            st.metric(
                                "📊 % da Classe",
                                f"{porcentagem_consumo_classe:.1f}%",
                                help="Porcentagem que esta frota representa do consumo total da classe"
                            )

                        # Gráfico de consumo por período
                        df_consumo_periodo = pd.DataFrame({
                            'Período': list(consumos_periodo.keys()),
                            'Consumo (L)': list(consumos_periodo.values())
                        })

                        # Melhorar formatação dos rótulos para o gráfico
                        df_consumo_periodo['Rótulo_Formatado'] = df_consumo_periodo['Consumo (L)'].apply(
                            lambda x: f"{formatar_brasileiro_int(x)} L" if x > 0 else "0 L"
                        )
                        
                        fig_consumo_periodo = px.bar(
                            df_consumo_periodo,
                            x='Período',
                            y='Consumo (L)',
                            title=f"Consumo de Combustível por Período - Frota {cod_sel}",
                            text='Rótulo_Formatado',
                            color='Consumo (L)',
                            color_continuous_scale='Blues'
                        )
                        
                        # Melhorar a aparência dos rótulos
                        fig_consumo_periodo.update_traces(
                            textposition='outside',
                            texttemplate='%{text}',
                            textfont=dict(
                                size=14,
                                color='#edf5fc',
                                family='Arial, sans-serif'
                            ),
                            hovertemplate='<b>%{x}</b><br>' +
                                        'Consumo: <b>%{y:,.0f} L</b><br>' +
                                        '<extra></extra>'
                        )
                        
                        fig_consumo_periodo.update_layout(
                            height=500,
                            showlegend=False,
                            xaxis_title="Período",
                            yaxis_title="Consumo (Litros)",
                            title_font=dict(size=18, color='#edf5fc'),
                            xaxis=dict(
                                title_font=dict(size=14, color='#edf5fc'),
                                tickfont=dict(size=12, color='#edf5fc')
                            ),
                            yaxis=dict(
                                title_font=dict(size=14, color='#edf5fc'),
                                tickfont=dict(size=12, color='#edf5fc'),
                                tickformat=',.0f'
                            ),
                            plot_bgcolor='rgba(0,0,0,0)',
                            paper_bgcolor='rgba(0,0,0,0)',
                            margin=dict(t=80, b=80, l=80, r=80)
                        )
                        st.plotly_chart(fig_consumo_periodo, use_container_width=True)

                        # Gráfico de comparação de consumo vs classe
                        if consumo_classe_total > 0:
                            df_comparacao_consumo = pd.DataFrame({
                                'Categoria': ['Esta Frota', 'Outras Frotas da Classe'],
                                'Consumo (L)': [consumo_total_litros, consumo_classe_total - consumo_total_litros]
                            })

                            # Melhorar formatação dos rótulos para o gráfico de pizza
                            df_comparacao_consumo['Rótulo_Formatado'] = df_comparacao_consumo['Consumo (L)'].apply(
                                lambda x: f"{formatar_brasileiro_int(x)} L"
                            )
                            
                            fig_consumo_classe = px.pie(
                                df_comparacao_consumo,
                                values='Consumo (L)',
                                names='Categoria',
                                title=f"Distribuição de Consumo na Classe {classe_selecionada}",
                                color_discrete_map={
                                    'Esta Frota': '#ff7f0e',
                                    'Outras Frotas da Classe': '#1f77b4'
                                }
                            )
                            
                            # Melhorar a aparência dos rótulos
                            fig_consumo_classe.update_traces(
                                textposition='inside',
                                textinfo='percent+label',
                                textfont=dict(
                                    size=16,
                                    color='white',
                                    family='Arial, sans-serif'
                                ),
                                hovertemplate='<b>%{label}</b><br>' +
                                            'Consumo: <b>%{value:,.0f} L</b><br>' +
                                            'Percentual: <b>%{percent:.1%}</b><br>' +
                                            '<extra></extra>'
                            )
                            
                            fig_consumo_classe.update_layout(
                                height=450,
                                title_font=dict(size=18, color='#2c3e50'),
                                showlegend=True,
                                legend=dict(
                                    font=dict(size=14, color='#34495e'),
                                    bgcolor='rgba(255,255,255,0.8)',
                                    bordercolor='#bdc3c7',
                                    borderwidth=1
                                ),
                                margin=dict(t=80, b=80, l=80, r=80)
                            )
                            st.plotly_chart(fig_consumo_classe, use_container_width=True)

                        # Gráfico de evolução mensal do consumo
                        if len(consumo_eq) > 1:
                            consumo_mensal_frota = consumo_eq.groupby('AnoMes')['Qtde_Litros'].sum().reset_index().sort_values('AnoMes')

                            if not consumo_mensal_frota.empty:
                                # Melhorar formatação dos dados para o gráfico
                                consumo_mensal_frota['Consumo_Formatado'] = consumo_mensal_frota['Qtde_Litros'].apply(
                                    lambda x: f"{formatar_brasileiro_int(x)} L"
                                )
                                
                                fig_evolucao = px.line(
                                    consumo_mensal_frota,
                                    x='AnoMes',
                                    y='Qtde_Litros',
                                    title=f"Evolução Mensal do Consumo - Frota {cod_sel}",
                                    labels={"AnoMes": "Mês/Ano", "Qtde_Litros": "Litros Consumidos"},
                                    markers=True,
                                    text='Consumo_Formatado'
                                )
                                
                                # Melhorar a aparência dos rótulos e marcadores
                                fig_evolucao.update_traces(
                                    textposition='top center',
                                    textfont=dict(
                                        size=12,
                                        color='#2c3e50',
                                        family='Arial, sans-serif'
                                    ),
                                    hovertemplate='<b>%{x}</b><br>' +
                                                'Consumo: <b>%{y:,.0f} L</b><br>' +
                                                '<extra></extra>',
                                    marker=dict(
                                        size=8,
                                        color='#e74c3c',
                                        line=dict(width=2, color='#edf5fc')
                                    ),
                                    line=dict(width=3, color='#e74c3c')
                                )
                                
                                fig_evolucao.update_layout(
                                    height=500,
                                    xaxis_title="Mês/Ano",
                                    yaxis_title="Litros Consumidos",
                                    title_font=dict(size=18, color='#edf5fc'),
                                    xaxis=dict(
                                        title_font=dict(size=14, color='#edf5fc'),
                                        tickfont=dict(size=12, color='#edf5fc'),
                                        tickangle=45
                                    ),
                                    yaxis=dict(
                                        title_font=dict(size=14, color='#edf5fc'),
                                        tickfont=dict(size=12, color='#edf5fc'),
                                        tickformat=',.0f'
                                    ),
                                    plot_bgcolor='rgba(0,0,0,0)',
                                    paper_bgcolor='rgba(0,0,0,0)',
                                    margin=dict(t=80, b=80, l=80, r=80)
                                )
                                st.plotly_chart(fig_evolucao, use_container_width=True)

                        # Resumo informativo
                        st.info(f"""
                        **📈 Resumo do Consumo:**
                        - **Total histórico:** {formatar_brasileiro_int(consumo_total_litros)} litros
                        - **Média por abastecimento:** {formatar_brasileiro_int(consumo_eq['Qtde_Litros'].mean())} litros
                        - **Total de abastecimentos:** {len(consumo_eq)} registros
                        - **Período de operação:** {consumo_eq['Data'].min().strftime('%d/%m/%Y')} a {consumo_eq['Data'].max().strftime('%d/%m/%Y')}
                        - **Comparação com classe:** Esta frota representa **{porcentagem_consumo_classe:.1f}%** do consumo total da classe **{classe_selecionada}**
                        """)
                    else:
                        st.info("Não há dados de consumo para este equipamento.")

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
                    
                    # Buscar componentes configurados para este equipamento
                    classe_equip = df_frotas[df_frotas['Cod_Equip'] == cod_sel]['Classe_Operacional'].iloc[0]
                    componentes_configurados = df_comp_regras[df_comp_regras['classe_operacional'] == classe_equip]
                    
                    if not componentes_configurados.empty:
                        # Criar abas para cada componente
                        componentes_nomes = componentes_configurados['nome_componente'].tolist()
                        if componentes_nomes:
                            tab_componentes = st.tabs(componentes_nomes)
                            
                            for i, componente in enumerate(componentes_nomes):
                                with tab_componentes[i]:
                                    # Buscar histórico do componente
                                    historico_componente = df_comp_historico[
                                        (df_comp_historico['Cod_Equip'] == cod_sel) & 
                                        (df_comp_historico['nome_componente'] == componente)
                                    ].sort_values("Data", ascending=False)
                                    
                                    # Buscar informações da regra do componente
                                    regra_componente = componentes_configurados[componentes_configurados['nome_componente'] == componente].iloc[0]
                                    intervalo_padrao = regra_componente['intervalo_padrao']
                                    
                                    # Buscar informações do lubrificante se existir
                                    lubrificante_info = ""
                                    if 'lubrificante_id' in regra_componente and regra_componente['lubrificante_id']:
                                        conn = sqlite3.connect(DB_PATH)
                                        df_lub = pd.read_sql("SELECT nome, viscosidade FROM lubrificantes WHERE id = ?", conn, params=(regra_componente['lubrificante_id'],))
                                        conn.close()
                                        if not df_lub.empty:
                                            lub = df_lub.iloc[0]
                                            lubrificante_info = f"{lub['nome']} ({lub['viscosidade']})"
                                    
                                    # Mostrar informações do componente
                                    col1, col2, col3 = st.columns(3)
                                    with col1:
                                        st.metric("Intervalo Padrão", f"{intervalo_padrao} {'km' if df_frotas[df_frotas['Cod_Equip'] == cod_sel]['Tipo_Controle'].iloc[0] == 'QUILÔMETROS' else 'h'}")
                                    with col2:
                                        if lubrificante_info:
                                            st.metric("Lubrificante", lubrificante_info)
                                        else:
                                            st.metric("Lubrificante", "Não aplicável")
                                    with col3:
                                        # Contar manutenções
                                        total_manutencoes = len(historico_componente)
                                        st.metric("Total de Manutenções", total_manutencoes)
                                    
                                    # Mostrar status atual se houver histórico
                                    if not historico_componente.empty:
                                        # Verificar se a coluna tipo_servico existe
                                        if 'tipo_servico' in historico_componente.columns:
                                            # Buscar a última TROCA (não remonta) para calcular km restantes
                                            ultimas_trocas = historico_componente[
                                                (historico_componente['tipo_servico'] == 'Troca') | 
                                                (historico_componente['tipo_servico'].isna())  # Para registros antigos sem tipo
                                            ]
                                        else:
                                            # Se a coluna não existe, considerar todos como trocas (registros antigos)
                                            ultimas_trocas = historico_componente
                                        
                                        if not ultimas_trocas.empty:
                                            ultima_troca = ultimas_trocas.iloc[0]
                                            hod_ultima_troca = ultima_troca['Hod_Hor_No_Servico']
                                            
                                            # Buscar hodômetro atual
                                            hod_atual = df[df['Cod_Equip'] == cod_sel]['Hod_Hor_Atual'].max()
                                            if pd.notna(hod_atual):
                                                km_restantes = (hod_ultima_troca + intervalo_padrao) - hod_atual
                                                
                                                col_status1, col_status2 = st.columns(2)
                                                with col_status1:
                                                    if km_restantes > 0:
                                                        st.success(f"🟢 **{formatar_brasileiro_int(km_restantes)}** restantes")
                                                    else:
                                                        st.error(f"🔴 **{formatar_brasileiro_int(abs(km_restantes))}** em atraso")
                                                
                                                with col_status2:
                                                    st.info(f"Última troca: {ultima_troca['Data']} ({formatar_brasileiro_int(hod_ultima_troca)})")
                                        else:
                                            st.warning("⚠️ Nenhuma troca registrada. Remontas não reiniciam o contador.")
                                        
                                        # Mostrar última manutenção (qualquer tipo)
                                        ultima_manutencao = historico_componente.iloc[0]
                                        tipo_ultima = ultima_manutencao.get('tipo_servico', 'N/A') if 'tipo_servico' in historico_componente.columns else 'N/A'
                                        st.info(f"Última manutenção: {ultima_manutencao['Data']} - {tipo_ultima}")
                                    
                                    # Mostrar histórico detalhado
                                    if not historico_componente.empty:
                                        st.subheader("Histórico Detalhado")
                                        # Selecionar colunas disponíveis
                                        colunas_disponiveis = ['Data', 'Hod_Hor_No_Servico', 'Observacoes']
                                        
                                        # Verificar e adicionar colunas se existirem
                                        if 'tipo_servico' in historico_componente.columns:
                                            colunas_disponiveis.insert(2, 'tipo_servico')
                                        if 'lubrificante_utilizado' in historico_componente.columns:
                                            colunas_disponiveis.insert(3, 'lubrificante_utilizado')
                                        
                                        # Filtrar apenas colunas que existem no DataFrame
                                        colunas_finais = [col for col in colunas_disponiveis if col in historico_componente.columns]
                                        
                                        st.dataframe(historico_componente[colunas_finais])
                                    else:
                                        st.info("Nenhum histórico de manutenção para este componente.")
                    else:
                        st.info("Nenhum componente configurado para esta classe de equipamento.")
                    
                    # --- INÍCIO DA MELHORIA 3: Estatísticas de Manutenção ---
                    st.markdown("---")
                    st.subheader("📊 Estatísticas de Manutenção por Tipo")
                    
                    # Buscar todas as manutenções do equipamento
                    todas_manutencoes = df_comp_historico[df_comp_historico['Cod_Equip'] == cod_sel]
                    
                    if not todas_manutencoes.empty:
                        # Verificar se a coluna tipo_servico existe
                        if 'tipo_servico' in todas_manutencoes.columns:
                            # Contar por tipo de serviço
                            contagem_tipos = todas_manutencoes['tipo_servico'].value_counts()
                        
                        col1, col2, col3 = st.columns(3)
                        
                        with col1:
                            total_manut = len(todas_manutencoes)
                            st.metric("Total de Manutenções", total_manut)
                        
                        with col2:
                            if 'tipo_servico' in todas_manutencoes.columns:
                                total_trocas = contagem_tipos.get('Troca', 0)
                                st.metric("Total de Trocas", total_trocas)
                            else:
                                st.metric("Total de Trocas", "N/A")
                        
                        with col3:
                            if 'tipo_servico' in todas_manutencoes.columns:
                                total_remontas = contagem_tipos.get('Remonta', 0)
                                st.metric("Total de Remontas", total_remontas)
                            else:
                                st.metric("Total de Remontas", "N/A")
                        
                        # Gráfico de pizza para tipos de manutenção
                        if 'tipo_servico' in todas_manutencoes.columns and len(contagem_tipos) > 0:
                            fig_tipos = px.pie(
                                values=contagem_tipos.values,
                                names=contagem_tipos.index,
                                title="Distribuição por Tipo de Manutenção"
                            )
                            st.plotly_chart(fig_tipos, use_container_width=True)
                        elif 'tipo_servico' not in todas_manutencoes.columns:
                            st.info("⚠️ Registros antigos detectados. Novas manutenções incluirão tipo de serviço.")
                    else:
                        st.info("Nenhuma manutenção registrada para este equipamento.")
                    # --- FIM DA MELHORIA 3 ---
                    # --- FIM DA MELHORIA 2 ---

                    st.subheader("Histórico de Abastecimentos")
                    # O seu histórico de abastecimentos continua igual
                    historico_abast_display = consumo_eq.sort_values("Data", ascending=False)
                    if not historico_abast_display.empty:
                        # Mostra matrícula (Cod_Equip) e nome do motorista quando disponível
                        colunas_abast = ["Data", "Qtde_Litros", "Media", "Hod_Hor_Atual", "Matricula", "Nome_Motorista"]
                        st.dataframe(historico_abast_display[[c for c in colunas_abast if c in historico_abast_display.columns]])
                    else:
                        st.info("Nenhum registo de abastecimento para este equipamento.")
                
                # ===== NOVA SEÇÃO: INSIGHTS E RECOMENDAÇÕES INTELIGENTES =====
                st.markdown("---")
                st.subheader("🧠 Insights e Recomendações Inteligentes")
                
                # Recalcular variáveis necessárias para os insights
                total_frotas_ativas_insights = df_frotas[df_frotas['ATIVO'] == 'ATIVO']['Cod_Equip'].nunique()
                frotas_com_alerta_insights = plan_df[plan_df['Qualquer_Alerta'] == True]['Cod_Equip'].nunique() if not plan_df.empty else 0
                
                # Calcular gasto total para insights
                gasto_total_combustivel_insights = 0
                if precos_map:
                    df_gastos_insights = df.copy()
                    if 'tipo_combustivel' in df_frotas.columns:
                        df_gastos_insights = df_gastos_insights.merge(df_frotas[['Cod_Equip','tipo_combustivel']], on='Cod_Equip', how='left')
                        df_gastos_insights['tipo_combustivel'] = df_gastos_insights['tipo_combustivel'].fillna('Diesel S500')
                    else:
                        df_gastos_insights['tipo_combustivel'] = 'Diesel S500'
                    
                    df_gastos_insights['preco_unit'] = df_gastos_insights['tipo_combustivel'].map(precos_map).fillna(0.0)
                    df_gastos_insights['custo'] = df_gastos_insights['Qtde_Litros'].fillna(0.0) * df_gastos_insights['preco_unit']
                    gasto_total_combustivel_insights = df_gastos_insights['custo'].sum()
                
                # Gerar insights baseados nos dados
                insights = []
                recomendacoes = []
                
                # Insight 1: Análise de eficiência
                if 'Media' in df.columns and not df['Media'].dropna().empty:
                    media_geral = df['Media'].mean()
                    equipamentos_ineficientes = df[df['Media'] > media_geral * 1.2]['Cod_Equip'].nunique()
                    if equipamentos_ineficientes > 0:
                        insights.append(f"🔍 **{equipamentos_ineficientes} equipamentos** estão consumindo mais de 20% acima da média")
                        recomendacoes.append("💡 Considere revisar a operação destes equipamentos ou agendar manutenção preventiva")
                
                # Insight 2: Análise de alertas
                if frotas_com_alerta_insights > 0:
                    insights.append(f"⚠️ **{frotas_com_alerta_insights} equipamentos** com alertas de manutenção pendentes")
                    recomendacoes.append("🔧 Priorize a manutenção destes equipamentos para evitar paradas não programadas")
                
                # Insight 3: Análise de gastos
                if gasto_total_combustivel_insights > 0:
                    # Calcular gasto mensal médio se houver dados de consumo mensal
                    if 'consumo_mensal' in locals() and len(consumo_mensal) > 0:
                        gasto_mensal_medio = gasto_total_combustivel_insights / len(consumo_mensal)
                        insights.append(f"💰 Gasto mensal médio com combustível: **{formatar_brasileiro(gasto_mensal_medio, 'R$ ')}**")
                        recomendacoes.append("📊 Monitore regularmente os gastos para identificar oportunidades de economia")
                    else:
                        insights.append(f"💰 Gasto total com combustível: **{formatar_brasileiro(gasto_total_combustivel_insights, 'R$ ')}**")
                        recomendacoes.append("📊 Monitore regularmente os gastos para identificar oportunidades de economia")
                
                # Insight 4: Análise de tendência (se disponível)
                if 'variacao' in locals() and abs(variacao) > 10:
                    if variacao > 0:
                        insights.append(f"📈 Consumo aumentou **{variacao:+.1f}%** no último período")
                        recomendacoes.append("🔍 Investigar causas do aumento de consumo (sazonalidade, operação, etc.)")
                    else:
                        insights.append(f"📉 Consumo diminuiu **{variacao:+.1f}%** no último período")
                        recomendacoes.append("✅ Boa performance! Mantenha as práticas que levaram à redução")
                
                # Insight 5: Análise de frotas ativas
                total_frotas = df_frotas['Cod_Equip'].nunique()
                if total_frotas_ativas_insights < total_frotas * 0.9:
                    frotas_inativas = total_frotas - total_frotas_ativas_insights
                    insights.append(f"🚫 **{frotas_inativas} equipamentos** estão inativos")
                    recomendacoes.append("🔄 Avalie se estes equipamentos podem ser reativados ou se devem ser descartados")
                
                # Exibir insights
                if insights:
                    st.markdown("**🔍 Insights Principais:**")
                    for insight in insights:
                        st.info(insight)
                else:
                    st.info("📊 Não há insights específicos para exibir no momento.")
                
                # Exibir recomendações
                if recomendacoes:
                    st.markdown("**💡 Recomendações:**")
                    for i, recomendacao in enumerate(recomendacoes, 1):
                        st.success(f"{i}. {recomendacao}")
                
                # Resumo executivo
                st.markdown("---")
                st.markdown("**📋 Resumo Executivo:**")
                
                col_res1, col_res2, col_res3 = st.columns(3)
                with col_res1:
                    st.metric("🚗 Frotas Analisadas", total_frotas_ativas_insights)
                with col_res2:
                    st.metric("💰 Gasto Total", formatar_brasileiro(gasto_total_combustivel_insights, 'R$ '))
                with col_res3:
                    st.metric("⚠️ Alertas Ativos", frotas_com_alerta_insights)
                
                # Botão para exportar relatório completo
                if st.button("📄 Gerar Relatório Executivo", type="primary"):
                    st.success("✅ Relatório gerado com sucesso! Use o botão de download abaixo.")
                    
                    # Criar relatório em formato de texto
                    relatorio = f"""
RELATÓRIO EXECUTIVO - ANÁLISE DE FROTA
Data: {datetime.now().strftime('%d/%m/%Y %H:%M')}

RESUMO GERAL:
- Frotas Ativas: {total_frotas_ativas_insights}
- Gasto Total com Combustível: {formatar_brasileiro(gasto_total_combustivel_insights, 'R$ ')}
- Equipamentos com Alerta: {frotas_com_alerta_insights}

INSIGHTS:
{chr(10).join([f"- {insight.replace('**', '')}" for insight in insights])}

RECOMENDAÇÕES:
{chr(10).join([f"- {rec}" for rec in recomendacoes])}

---
Relatório gerado automaticamente pelo sistema de gestão de frotas.
                    """
                    
                    # Botão de download do relatório
                    st.download_button(
                        "📥 Download Relatório Executivo",
                        relatorio.encode('utf-8'),
                        "relatorio_executivo_frota.txt",
                        "text/plain"
                    )
                                
        if tab_manut is not None:
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

                else:
                    st.info("Não há dados suficientes para gerar o plano de manutenção.")

                st.markdown("---")
                st.subheader("🛠️ Controle de Manutenção por Componentes")

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
                    componente_info = {}
                    if equip_label:
                        cod_equip_selecionado = int(equip_label.split(" - ")[0])
                        classe_selecionada = df_frotas.loc[df_frotas['Cod_Equip'] == cod_equip_selecionado, 'Classe_Operacional'].iloc[0]
                        regras_classe = df_comp_regras[df_comp_regras['classe_operacional'] == classe_selecionada]
                        if not regras_classe.empty:
                            componentes_disponiveis = regras_classe['nome_componente'].tolist()
                            # Criar dicionário com informações dos componentes
                            for _, regra in regras_classe.iterrows():
                                componente_info[regra['nome_componente']] = {
                                    'intervalo': regra['intervalo_padrao'],
                                    'lubrificante_id': regra.get('lubrificante_id'),
                                    'tipo_manutencao': regra.get('tipo_manutencao', 'Troca')
                                }
                    
                    componente_servico = st.selectbox("Componente que recebeu manutenção", options=componentes_disponiveis)
                    
                    # Mostrar informações do componente selecionado
                    if componente_servico and componente_servico in componente_info:
                        info = componente_info[componente_servico]
                        st.info(f"**Componente:** {componente_servico} | **Intervalo:** {info['intervalo']} | **Tipo:** {info['tipo_manutencao']}")
                        
                        # Buscar informações do lubrificante se existir
                        if info['lubrificante_id']:
                            conn = sqlite3.connect(DB_PATH)
                            df_lub = pd.read_sql("SELECT nome, viscosidade, quantidade_estoque, unidade FROM lubrificantes WHERE id = ?", conn, params=(info['lubrificante_id'],))
                            conn.close()
                            if not df_lub.empty:
                                lub = df_lub.iloc[0]
                                estoque_atual = lub.get('quantidade_estoque', 0)
                                unidade = lub.get('unidade', 'L')
                                
                                # Determinar cor do estoque
                                if estoque_atual > 10:
                                    cor_estoque = "🟢"
                                elif estoque_atual > 3:
                                    cor_estoque = "🟡"
                                else:
                                    cor_estoque = "🔴"
                                
                                st.info(f"**Lubrificante associado:** {lub['nome']} ({lub['viscosidade']}) | **Estoque:** {cor_estoque} {estoque_atual} {unidade}")
                    
                    col1, col2 = st.columns(2)
                    with col1:
                        data_servico = st.date_input("Data do Serviço")
                        hod_hor_servico = st.number_input("Leitura no Momento do Serviço", min_value=0.0, format="%.2f")
                    
                    with col2:
                        # Tipo de serviço baseado na configuração do componente
                        tipo_servico_opcoes = []
                        if componente_servico and componente_servico in componente_info:
                            tipo_config = componente_info[componente_servico]['tipo_manutencao']
                            if tipo_config == "Troca":
                                tipo_servico_opcoes = ["Troca"]
                            elif tipo_config == "Remonta":
                                tipo_servico_opcoes = ["Remonta"]
                            elif tipo_config == "Ambos":
                                tipo_servico_opcoes = ["Troca", "Remonta"]
                        else:
                            tipo_servico_opcoes = ["Troca", "Remonta"]
                        
                        tipo_servico = st.selectbox("Tipo de Serviço", options=tipo_servico_opcoes)
                        
                        # Lubrificante utilizado (se aplicável)
                        lubrificante_utilizado = None
                        if componente_servico and componente_servico in componente_info and info['lubrificante_id']:
                            conn = sqlite3.connect(DB_PATH)
                            df_lub = pd.read_sql("SELECT nome, quantidade_estoque, unidade FROM lubrificantes WHERE id = ?", conn, params=(info['lubrificante_id'],))
                            conn.close()
                            if not df_lub.empty:
                                lub = df_lub.iloc[0]
                                lubrificante_utilizado = lub['nome']
                                estoque_atual = lub.get('quantidade_estoque', 0)
                                unidade = lub.get('unidade', 'L')
                                
                                # Determinar cor do estoque
                                if estoque_atual > 10:
                                    cor_estoque = "🟢"
                                elif estoque_atual > 3:
                                    cor_estoque = "🟡"
                                else:
                                    cor_estoque = "🔴"
                                
                                st.info(f"**Lubrificante:** {lubrificante_utilizado} | **Estoque:** {cor_estoque} {estoque_atual} {unidade}")
                                
                                # Aviso se estoque baixo
                                if estoque_atual <= 3:
                                    st.warning(f"⚠️ **Estoque baixo!** Considere repor o lubrificante '{lubrificante_utilizado}'")
                    
                    observacoes = st.text_area("Observações (opcional)", placeholder="Detalhes do serviço realizado...")

                    if st.form_submit_button("Salvar Manutenção de Componente"):
                        if equip_label and componente_servico:
                            cod_equip = int(equip_label.split(" - ")[0])
                            
                            # Usar a nova função avançada
                            success, message = add_component_service_advanced(
                                cod_equip, 
                                componente_servico, 
                                data_servico.strftime("%Y-%m-%d"), 
                                hod_hor_servico, 
                                tipo_servico, 
                                lubrificante_utilizado, 
                                observacoes
                            )
                            
                            if success:
                                st.success(f"Manutenção do componente '{componente_servico}' para '{equip_label}' registrada com sucesso!")
                                
                                # Atualizar estoque de lubrificante se aplicável
                                if lubrificante_utilizado and tipo_servico in ["Troca", "Remonta"]:
                                    try:
                                        conn = sqlite3.connect(DB_PATH)
                                        cursor = conn.cursor()
                                        
                                        # Buscar a capacidade do componente
                                        cursor.execute(
                                            "SELECT capacidade_litros FROM componentes_regras WHERE nome_componente = ? AND classe_operacional = (SELECT Classe_Operacional FROM frotas WHERE Cod_Equip = ?)",
                                            (componente_servico, cod_equip)
                                        )
                                        result = cursor.fetchone()
                                        capacidade = result[0] if result and result[0] else 1.0  # Padrão 1L se não definido
                                        
                                        # Reduzir estoque do lubrificante baseado na capacidade
                                        cursor.execute(
                                            "UPDATE lubrificantes SET quantidade_estoque = quantidade_estoque - ? WHERE nome = ?",
                                            (capacidade, lubrificante_utilizado)
                                        )
                                        conn.commit()
                                        conn.close()
                                        # Buscar o novo estoque após a atualização
                                        cursor.execute(
                                            "SELECT quantidade_estoque, unidade FROM lubrificantes WHERE nome = ?",
                                            (lubrificante_utilizado,)
                                        )
                                        result_estoque = cursor.fetchone()
                                        novo_estoque = result_estoque[0] if result_estoque else 0
                                        unidade_estoque = result_estoque[1] if result_estoque else 'L'
                                        
                                        # Determinar cor do novo estoque
                                        if novo_estoque > 10:
                                            cor_novo_estoque = "🟢"
                                        elif novo_estoque > 3:
                                            cor_novo_estoque = "🟡"
                                        else:
                                            cor_novo_estoque = "🔴"
                                        
                                        st.info(f"Estoque do lubrificante '{lubrificante_utilizado}' atualizado ({tipo_servico}) - {capacidade}L consumidos. **Novo estoque:** {cor_novo_estoque} {novo_estoque} {unidade_estoque}")
                                        
                                        # Aviso se estoque ficou baixo após a operação
                                        if novo_estoque <= 3:
                                            st.warning(f"⚠️ **Atenção!** Estoque do lubrificante '{lubrificante_utilizado}' está baixo ({novo_estoque} {unidade_estoque}). Considere repor.")
                                    except Exception as e:
                                        st.warning(f"Não foi possível atualizar o estoque do lubrificante: {e}")
                                
                                # Atualizar cache para refletir mudanças
                                st.cache_data.clear()
                                rerun_keep_tab("🛠️ Controle de Manutenção")
                            else:
                                st.error(f"Erro ao salvar manutenção: {message}")
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

        if tab_checklists is not None:
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
                    
        if st.session_state.role == 'admin' and tab_gerir_lanc is not None:
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
                                # Seleção de motorista (matrícula)
                                df_mot_all = get_all_motoristas()
                                matriculas_opts = [m for m in df_mot_all['matricula'].astype(str).tolist()] if not df_mot_all.empty else []
                                matricula_sel = st.selectbox("Matrícula do Motorista", options=[""] + matriculas_opts)

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
                                        
                                        # Usa o nome da coluna padronizado ('Classe_Operacional' com underscore)
                                        classe_op = df_frotas.loc[df_frotas['Cod_Equip'] == cod_equip, 'Classe_Operacional'].iloc[0]

                                        cod_pessoa_val = None
                                        if matricula_sel:
                                            df_mot_sel = df_mot_all[df_mot_all['matricula'].astype(str) == str(matricula_sel)] if not df_mot_all.empty else pd.DataFrame()
                                            if not df_mot_sel.empty:
                                                cod_pessoa_val = df_mot_sel.iloc[0].get('codigo_pessoa')

                                        dados_novos = {
                                            'cod_equip': cod_equip,
                                            'data': data_abastecimento.strftime("%Y-%m-%d %H:%M:%S"),
                                            'qtde_litros': qtde_litros,
                                            'hod_hor_atual': hod_hor_atual,
                                            'safra': safra,
                                            'mes': data_abastecimento.month,
                                            'classe_operacional': classe_op,
                                            'matricula': matricula_sel if matricula_sel else None,
                                            'cod_pessoa': cod_pessoa_val
                                        }

                                        if inserir_abastecimento(DB_PATH, dados_novos):
                                            st.success("Abastecimento salvo com sucesso!")
                                            rerun_keep_tab("⚙️ Gerir Lançamentos")

                        elif acao == "Excluir Lançamento":
                                    st.subheader("🗑️ Excluir um Lançamento")
                                    
                                    tipo_exclusao = st.radio("O que deseja excluir?", ("Abastecimento", "Manutenção", "Manutenção de Componentes"), horizontal=True, key="delete_choice")
                                    
                                    if tipo_exclusao == "Abastecimento":
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
                                                    # Invalidar cache para atualizar contadores
                                                    st.cache_data.clear()
                                                    rerun_keep_tab("⚙️ Gerir Lançamentos")
                                    
                                    elif tipo_exclusao == "Manutenção":
                                        st.subheader("🗑️ Excluir Manutenção")
                                        
                                        # Garantir que df_manutencoes tenha rowid
                                        if 'rowid' not in df_manutencoes.columns:
                                            df_manutencoes = df_manutencoes.reset_index().rename(columns={'index': 'rowid'})
                                        
                                        df_manut_para_excluir = df_manutencoes.copy()
                                        df_manut_para_excluir['Data'] = pd.to_datetime(df_manut_para_excluir['Data'], errors='coerce')
                                        df_manut_para_excluir = df_manut_para_excluir.sort_values(by="Data", ascending=False)
                                        
                                        # Adiciona descrição do equipamento
                                        df_frotas_unique = df_frotas.drop_duplicates(subset=['Cod_Equip'], keep='first')
                                        desc_map = df_frotas_unique.set_index('Cod_Equip')['DESCRICAO_EQUIPAMENTO']
                                        df_manut_para_excluir['DESCRICAO_EQUIPAMENTO'] = df_manut_para_excluir['Cod_Equip'].map(desc_map).fillna('N/A')
                                        
                                        df_manut_para_excluir['label_exclusao'] = (
                                            df_manut_para_excluir['Data'].dt.strftime('%d/%m/%Y') + " | Frota: " +
                                            df_manut_para_excluir['Cod_Equip'].astype(str) + " - " +
                                            df_manut_para_excluir['DESCRICAO_EQUIPAMENTO'].fillna('N/A') + " | " +
                                            df_manut_para_excluir['Tipo_Servico'] + " | " +
                                            df_manut_para_excluir['Hod_Hor_No_Servico'].apply(lambda x: formatar_brasileiro_int(x)) + " h/km"
                                        )
                                        
                                        map_label_to_rowid = pd.Series(df_manut_para_excluir.rowid.values, index=df_manut_para_excluir.label_exclusao).to_dict()
                                        
                                        registro_selecionado_label = st.selectbox(
                                            "Selecione a manutenção a ser excluída (mais recentes primeiro)",
                                            options=df_manut_para_excluir['label_exclusao']
                                        )
                                        
                                        if registro_selecionado_label:
                                            rowid_para_excluir = map_label_to_rowid[registro_selecionado_label]
                                            
                                            st.warning("**Atenção:** Você está prestes a excluir o seguinte registro. Esta ação não pode ser desfeita.")
                                            
                                            registro_detalhes = df_manut_para_excluir[df_manut_para_excluir['rowid'] == rowid_para_excluir]
                                            st.dataframe(registro_detalhes[['Data', 'DESCRICAO_EQUIPAMENTO', 'Tipo_Servico', 'Hod_Hor_No_Servico']])
            
                                            if st.button("Confirmar Exclusão", type="primary"):
                                                if excluir_manutencao(DB_PATH, rowid_para_excluir):
                                                    st.success("Manutenção excluída com sucesso!")
                                                    # Invalidar cache para atualizar contadores
                                                    st.cache_data.clear()
                                                    rerun_keep_tab("⚙️ Gerir Lançamentos")
                                    
                                    elif tipo_exclusao == "Manutenção de Componentes":
                                        st.subheader("🗑️ Excluir Manutenção de Componentes")
                                        
                                        df_comp_para_excluir = df_comp_historico.copy()
                                        
                                        if df_comp_para_excluir.empty:
                                            st.warning("Nenhuma manutenção de componente encontrada.")
                                        else:
                                            # Garantir que df_comp_historico tenha rowid
                                            if 'rowid' not in df_comp_para_excluir.columns:
                                                df_comp_para_excluir = df_comp_para_excluir.reset_index().rename(columns={'index': 'rowid'})
                                            
                                            df_comp_para_excluir['Data'] = pd.to_datetime(df_comp_para_excluir['Data'], errors='coerce')
                                            df_comp_para_excluir = df_comp_para_excluir.sort_values(by="Data", ascending=False)
                                            
                                            # Adiciona descrição do equipamento
                                            df_frotas_unique = df_frotas.drop_duplicates(subset=['Cod_Equip'], keep='first')
                                            desc_map = df_frotas_unique.set_index('Cod_Equip')['DESCRICAO_EQUIPAMENTO']
                                            df_comp_para_excluir['DESCRICAO_EQUIPAMENTO'] = df_comp_para_excluir['Cod_Equip'].map(desc_map).fillna('N/A')
                                            
                                            df_comp_para_excluir['label_exclusao'] = (
                                                df_comp_para_excluir['Data'].dt.strftime('%d/%m/%Y') + " | Frota: " +
                                                df_comp_para_excluir['Cod_Equip'].astype(str) + " - " +
                                                df_comp_para_excluir['DESCRICAO_EQUIPAMENTO'].fillna('N/A') + " | " +
                                                df_comp_para_excluir['nome_componente'] + " | " +
                                                df_comp_para_excluir['Observacoes'].fillna('N/A')
                                            )
                                            
                                            map_label_to_rowid = pd.Series(df_comp_para_excluir.rowid.values, index=df_comp_para_excluir.label_exclusao).to_dict()
                                            
                                            registro_selecionado_label = st.selectbox(
                                                "Selecione a manutenção de componente a ser excluída (mais recentes primeiro)",
                                                options=df_comp_para_excluir['label_exclusao']
                                            )
                                            
                                            if registro_selecionado_label:
                                                rowid_para_excluir = map_label_to_rowid[registro_selecionado_label]
                                                
                                                st.warning("**Atenção:** Você está prestes a excluir o seguinte registro. Esta ação não pode ser desfeita.")
                                                
                                                registro_detalhes = df_comp_para_excluir[df_comp_para_excluir['rowid'] == rowid_para_excluir]
                                                st.dataframe(registro_detalhes[['Data', 'DESCRICAO_EQUIPAMENTO', 'nome_componente', 'Observacoes']])
                
                                                if st.button("Confirmar Exclusão", type="primary"):
                                                    # Obter os dados do registro selecionado
                                                    registro_detalhes = df_comp_para_excluir[df_comp_para_excluir['rowid'] == rowid_para_excluir].iloc[0]
                                                    
                                                    # Converter a data para string se for Timestamp
                                                    data_str = str(registro_detalhes['Data'])
                                                    if hasattr(registro_detalhes['Data'], 'strftime'):
                                                        data_str = registro_detalhes['Data'].strftime('%Y-%m-%d')
                                                    
                                                    if excluir_manutencao_componente(
                                                        DB_PATH, 
                                                        registro_detalhes['Cod_Equip'],
                                                        registro_detalhes['nome_componente'],
                                                        data_str,
                                                        registro_detalhes['Hod_Hor_No_Servico']
                                                    ):
                                                        st.success("Manutenção de componente excluída com sucesso!")
                                                        # Invalidar cache para atualizar contadores
                                                        force_cache_clear()
                                                        rerun_keep_tab("⚙️ Gerir Lançamentos")
                                                
                        elif acao == "Editar Lançamento":
                                    st.subheader("✏️ Editar um Lançamento")
                                    tipo_edicao = st.radio("O que deseja editar?", ("Abastecimento", "Manutenção", "Manutenção de Componentes"), horizontal=True, key="edit_choice")
            
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
                                                # Motorista: matrícula e nome (mostra matrícula na UI e guarda ambos)
                                                df_mot_all = get_all_motoristas()
                                                matriculas_opts = [m for m in df_mot_all['matricula'].astype(str).tolist()] if not df_mot_all.empty else []
                                                matricula_sel = st.selectbox("Matrícula do Motorista", options=[""] + matriculas_opts)

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
                                                # map matricula -> cod_pessoa/nome
                                                cod_pessoa_val = None
                                                if matricula_sel:
                                                    df_mot_sel = df_mot_all[df_mot_all['matricula'].astype(str) == str(matricula_sel)] if not df_mot_all.empty else pd.DataFrame()
                                                    if not df_mot_sel.empty:
                                                        cod_pessoa_val = df_mot_sel.iloc[0].get('codigo_pessoa')
                                                dados_editados = {
                                                    'cod_equip': int(novo_equip_label.split(" - ")[0]),
                                                    'data': nova_data.strftime("%Y-%m-%d %H:%M:%S"), 
                                                    'qtde_litros': nova_qtde,
                                                    'hod_hor_atual': novo_hod,
                                                    'safra': nova_safra,
                                                    'matricula': matricula_sel if matricula_sel else None,
                                                    'cod_pessoa': cod_pessoa_val
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

                                    if tipo_edicao == "Manutenção de Componentes":
                                        st.subheader("Editar Lançamento de Manutenção de Componentes")

                                        # Carregar dados de componentes_historico
                                        df_comp_edit = df_comp_historico.copy()
                                        
                                        if df_comp_edit.empty:
                                            st.warning("Nenhuma manutenção de componente encontrada.")
                                        else:
                                            # Garantir que df_comp_historico tenha rowid
                                            if 'rowid' not in df_comp_edit.columns:
                                                df_comp_edit = df_comp_edit.reset_index().rename(columns={'index': 'rowid'})

                                            # Garante que a coluna Data seja datetime
                                            df_comp_edit['Data'] = pd.to_datetime(df_comp_edit['Data'], errors='coerce')

                                            # Remove duplicatas de Cod_Equip no df_frotas para evitar erro no map
                                            df_frotas_unique = df_frotas.drop_duplicates(subset=['Cod_Equip'], keep='first')

                                            # Adiciona descrição do equipamento via map
                                            desc_map = df_frotas_unique.set_index('Cod_Equip')['DESCRICAO_EQUIPAMENTO']
                                            df_comp_edit['DESCRICAO_EQUIPAMENTO'] = df_comp_edit['Cod_Equip'].map(desc_map).fillna('N/A')

                                            # Ordena e cria os labels para seleção com informações melhoradas
                                            df_comp_edit.sort_values(by="Data", ascending=False, inplace=True)
                                            
                                            # Adicionar informações de tipo de serviço e lubrificante se disponíveis
                                            if 'tipo_servico' in df_comp_edit.columns:
                                                df_comp_edit['tipo_servico_info'] = df_comp_edit['tipo_servico'].fillna('N/A')
                                            else:
                                                df_comp_edit['tipo_servico_info'] = 'N/A'
                                            
                                            if 'lubrificante_utilizado' in df_comp_edit.columns:
                                                df_comp_edit['lubrificante_info'] = df_comp_edit['lubrificante_utilizado'].fillna('N/A')
                                            else:
                                                df_comp_edit['lubrificante_info'] = 'N/A'
                                            
                                            df_comp_edit['label_edit'] = (
                                                df_comp_edit['Data'].dt.strftime('%d/%m/%Y') + " | " +
                                                df_comp_edit['Cod_Equip'].astype(str) + " - " +
                                                df_comp_edit['DESCRICAO_EQUIPAMENTO'].fillna('N/A') + " | " +
                                                df_comp_edit['nome_componente'] + " | " +
                                                df_comp_edit['tipo_servico_info'] + " | " +
                                                df_comp_edit['lubrificante_info']
                                            )

                                            # Cria o dicionário de label -> rowid
                                            map_label_to_rowid = pd.Series(
                                                df_comp_edit['rowid'].values,
                                                index=df_comp_edit['label_edit']
                                            ).to_dict()

                                            # Selectbox para escolher manutenção de componente
                                            label_selecionado = st.selectbox(
                                                "Selecione a manutenção de componente para editar",
                                                options=df_comp_edit['label_edit'],
                                                key="comp_edit_select"
                                            )

                                            if label_selecionado:
                                                rowid_selecionado = map_label_to_rowid.get(label_selecionado)
                                                if rowid_selecionado is not None:
                                                    dados_atuais = df_comp_edit[df_comp_edit['rowid'] == rowid_selecionado].iloc[0]

                                                    with st.form("form_edit_comp"):
                                                        st.write(f"**Editando:** {label_selecionado}")

                                                        col1, col2 = st.columns(2)
                                                        
                                                        with col1:
                                                            lista_labels_frotas = df_frotas.sort_values("label")['label'].tolist()
                                                            equip_atual = df_frotas[df_frotas['Cod_Equip'] == dados_atuais['Cod_Equip']]['label'].iloc[0]
                                                            index_equip_atual = lista_labels_frotas.index(equip_atual)

                                                            novo_equip_label = st.selectbox("Equipamento", options=lista_labels_frotas, index=index_equip_atual)
                                                            novo_componente = st.text_input("Componente", value=dados_atuais['nome_componente'])
                                                            nova_data = st.date_input("Data", value=pd.to_datetime(dados_atuais['Data']).date())
                                                            novo_hod = st.number_input("Hod./Hor. no Serviço", value=float(dados_atuais.get('Hod_Hor_No_Servico', 0)), format="%.2f")
                                                        
                                                        with col2:
                                                            # Tipo de serviço
                                                            if 'tipo_servico' in dados_atuais:
                                                                tipo_servico_atual = dados_atuais.get('tipo_servico', 'Troca')
                                                                tipo_servico_opcoes = ["Troca", "Remonta"]
                                                                index_tipo = tipo_servico_opcoes.index(tipo_servico_atual) if tipo_servico_atual in tipo_servico_opcoes else 0
                                                                novo_tipo_servico = st.selectbox("Tipo de Serviço", options=tipo_servico_opcoes, index=index_tipo)
                                                            else:
                                                                novo_tipo_servico = st.selectbox("Tipo de Serviço", options=["Troca", "Remonta"], index=0)
                                                            
                                                            # Lubrificante utilizado
                                                            if 'lubrificante_utilizado' in dados_atuais:
                                                                lubrificante_atual = dados_atuais.get('lubrificante_utilizado', '')
                                                                novo_lubrificante = st.text_input("Lubrificante Utilizado", value=lubrificante_atual)
                                                            else:
                                                                novo_lubrificante = st.text_input("Lubrificante Utilizado", value="")
                                                            
                                                            nova_acao = st.text_input("Observações", value=dados_atuais.get('Observacoes', ''))

                                                        submitted = st.form_submit_button("Salvar Alterações")
                                                        if submitted:
                                                            dados_editados = {
                                                                'cod_equip': int(novo_equip_label.split(" - ")[0]),
                                                                'componente': novo_componente,
                                                                'acao': nova_acao,
                                                                'data': nova_data.strftime("%Y-%m-%d"),
                                                                'hod_hor_servico': novo_hod,
                                                                'tipo_servico': novo_tipo_servico,
                                                                'lubrificante_utilizado': novo_lubrificante if novo_lubrificante else None,
                                                            }
                                                            if editar_manutencao_componente_advanced(DB_PATH, rowid_selecionado, dados_editados):
                                                                st.success("Manutenção de componente atualizada com sucesso!")
                                                                rerun_keep_tab("⚙️ Gerir Lançamentos")

            if tab_gerir_lub is not None:
                with tab_gerir_lub:
                        st.header("🛢️ Gestão de Lubrificantes")
                        ensure_lubrificantes_schema()
                        conn = sqlite3.connect(DB_PATH)
                        df_lub = pd.read_sql("SELECT * FROM lubrificantes", conn)
                        df_mov = pd.read_sql("SELECT * FROM lubrificantes_movimentacoes", conn)
                        df_almoxarifados = get_almoxarifados()

                        # ----------- Visualização do Estoque Atual -----------
                        st.subheader("Visualização do Estoque Atual")

                        if not df_lub.empty:
                            # Separar por tipo
                            df_oleos = df_lub[df_lub['tipo'].str.lower() == 'óleo']
                            df_graxas = df_lub[df_lub['tipo'].str.lower() == 'graxa']

                            col_o, col_g = st.columns(2)
                            with col_o:
                                st.markdown("#### Estoque de Óleos")
                                if not df_oleos.empty:
                                    fig_oleos = px.bar(
                                        df_oleos,
                                        x='nome',
                                        y='quantidade_estoque',
                                        color='viscosidade',
                                        text='quantidade_estoque',
                                        title="Óleos - Estoque Atual",
                                        labels={'quantidade_estoque': 'Qtd. Estoque', 'nome': 'Óleo'}
                                    )
                                    st.plotly_chart(fig_oleos, use_container_width=True)
                                else:
                                    st.info("Nenhum óleo cadastrado.")

                            with col_g:
                                st.markdown("#### Estoque de Graxas")
                                if not df_graxas.empty:
                                    fig_graxas = px.bar(
                                        df_graxas,
                                        x='nome',
                                        y='quantidade_estoque',
                                        color='viscosidade',
                                        text='quantidade_estoque',
                                        title="Graxas - Estoque Atual",
                                        labels={'quantidade_estoque': 'Qtd. Estoque', 'nome': 'Graxa'}
                                    )
                                    st.plotly_chart(fig_graxas, use_container_width=True)
                                else:
                                    st.info("Nenhuma graxa cadastrada.")

                            # Pizza geral
                            df_lub['tipo'] = df_lub['tipo'].fillna('óleo')
                            fig_pizza = px.pie(
                                df_lub,
                                names='tipo',
                                values='quantidade_estoque',
                                title="Proporção de Estoque: Óleos vs Graxas"
                            )
                            st.plotly_chart(fig_pizza, use_container_width=True)

                            st.markdown("#### Tabela Detalhada do Estoque")
                            st.dataframe(df_lub)
                        else:
                            st.info("Nenhum lubrificante cadastrado.")

                        st.markdown("---")

                        # ----------- KPI de Estoque Distribuído -----------
                        st.subheader("📊 KPI de Estoque Distribuído por Almoxarifado")
                        
                        if not df_lub.empty and not df_almoxarifados.empty:
                            # Seleção do lubrificante para análise
                            lub_analise = st.selectbox(
                                "Selecione o lubrificante para análise de estoque distribuído:",
                                options=df_lub['nome'].tolist(),
                                key="kpi_lubrificante"
                            )
                            
                            if lub_analise:
                                id_lub_analise = df_lub[df_lub['nome'] == lub_analise]['id'].iloc[0]
                                df_estoque_distribuido = get_estoque_por_almoxarifado(id_lub_analise)
                                
                                if not df_estoque_distribuido.empty:
                                    # Calcular totais
                                    total_estoque = df_estoque_distribuido['quantidade'].sum()
                                    total_almoxarifados = len(df_estoque_distribuido)
                                    
                                    # Métricas principais
                                    col_kpi1, col_kpi2, col_kpi3, col_kpi4 = st.columns(4)
                                    
                                    with col_kpi1:
                                        st.metric("Estoque Total", f"{total_estoque:.1f} {df_estoque_distribuido['unidade'].iloc[0]}")
                                    
                                    with col_kpi2:
                                        st.metric("Almoxarifados", total_almoxarifados)
                                    
                                    with col_kpi3:
                                        almoxarifados_com_estoque = len(df_estoque_distribuido[df_estoque_distribuido['quantidade'] > 0])
                                        st.metric("Com Estoque", almoxarifados_com_estoque)
                                    
                                    with col_kpi4:
                                        almoxarifados_sem_estoque = total_almoxarifados - almoxarifados_com_estoque
                                        st.metric("Sem Estoque", almoxarifados_sem_estoque)
                                    
                                    # Gráfico de distribuição
                                    col_grafico, col_tabela = st.columns([2, 1])
                                    
                                    with col_grafico:
                                        # Preparar dados para o gráfico
                                        df_grafico = df_estoque_distribuido[df_estoque_distribuido['quantidade'] > 0].copy()
                                        if not df_grafico.empty:
                                            df_grafico['percentual'] = (df_grafico['quantidade'] / total_estoque * 100).round(1)
                                            
                                            fig_distribuicao = px.pie(
                                                df_grafico,
                                                values='quantidade',
                                                names='almoxarifado',
                                                title=f"Distribuição de Estoque: {lub_analise}",
                                                hover_data=['percentual', 'quantidade', 'unidade']
                                            )
                                            fig_distribuicao.update_traces(textposition='inside', textinfo='percent+label')
                                            st.plotly_chart(fig_distribuicao, use_container_width=True)
                                        else:
                                            st.info("Nenhum almoxarifado possui estoque deste lubrificante.")
                                    
                                    with col_tabela:
                                        st.write("**Detalhamento por Almoxarifado:**")
                                        
                                        # Preparar dados para tabela
                                        df_tabela = df_estoque_distribuido.copy()
                                        df_tabela['percentual'] = (df_tabela['quantidade'] / total_estoque * 100).round(1)
                                        df_tabela['quantidade_formatada'] = df_tabela['quantidade'].astype(str) + ' ' + df_tabela['unidade']
                                        
                                        # Determinar cor baseada na quantidade
                                        def get_color(quantidade, total):
                                            if total == 0:
                                                return "⚪"
                                            percentual = (quantidade / total * 100) if total > 0 else 0
                                            if percentual > 50:
                                                return "🟢"
                                            elif percentual > 20:
                                                return "🟡"
                                            else:
                                                return "🔴"
                                        
                                        df_tabela['indicador'] = df_tabela.apply(
                                            lambda row: get_color(row['quantidade'], total_estoque), axis=1
                                        )
                                        
                                        # Exibir tabela
                                        for _, row in df_tabela.iterrows():
                                            if row['quantidade'] > 0:
                                                st.write(f"{row['indicador']} **{row['almoxarifado']}**")
                                                st.write(f"   {row['quantidade_formatada']} ({row['percentual']}%)")
                                                if row['tipo'] == 'movel':
                                                    st.write(f"   🚛 {row['localizacao']}")
                                                else:
                                                    st.write(f"   🏢 {row['localizacao']}")
                                                st.write("---")
                                            else:
                                                st.write(f"⚪ **{row['almoxarifado']}**")
                                                st.write(f"   Sem estoque")
                                                st.write("---")
                                    
                                    # Gráfico de barras por tipo de almoxarifado
                                    st.subheader("📈 Análise por Tipo de Almoxarifado")
                                    
                                    df_tipo = df_estoque_distribuido.groupby('tipo').agg({
                                        'quantidade': 'sum',
                                        'almoxarifado': 'count'
                                    }).reset_index()
                                    df_tipo['percentual'] = (df_tipo['quantidade'] / total_estoque * 100).round(1)
                                    
                                    col_barras1, col_barras2 = st.columns(2)
                                    
                                    with col_barras1:
                                        fig_barras_tipo = px.bar(
                                            df_tipo,
                                            x='tipo',
                                            y='quantidade',
                                            title="Estoque por Tipo de Almoxarifado",
                                            labels={'tipo': 'Tipo', 'quantidade': 'Quantidade'},
                                            text='quantidade'
                                        )
                                        st.plotly_chart(fig_barras_tipo, use_container_width=True)
                                    
                                    with col_barras2:
                                        fig_barras_contagem = px.bar(
                                            df_tipo,
                                            x='tipo',
                                            y='almoxarifado',
                                            title="Quantidade de Almoxarifados por Tipo",
                                            labels={'tipo': 'Tipo', 'almoxarifado': 'Quantidade'},
                                            text='almoxarifado'
                                        )
                                        st.plotly_chart(fig_barras_contagem, use_container_width=True)
                                    
                                else:
                                    st.warning("Nenhum almoxarifado cadastrado para análise.")
                        else:
                            if df_lub.empty:
                                st.warning("Cadastre lubrificantes para visualizar o KPI de estoque distribuído.")
                            if df_almoxarifados.empty:
                                st.warning("Cadastre almoxarifados para visualizar o KPI de estoque distribuído.")

                        st.markdown("---")

                        # ----------- Registro de Entrada/Saída -----------
                        st.subheader("📦 Registrar Entrada/Saída de Lubrificantes")
                        
                        # Criar abas para entrada e saída
                        tab_entrada, tab_saida = st.tabs(["➕ Entrada (Compra)", "➖ Saída (Consumo)"])
                        
                        if not df_lub.empty:
                            # Aba de Entrada (Compra)
                            with tab_entrada:
                                st.info("💡 **Entrada:** Registre compras e reposições de lubrificantes no almoxarifado.")
                                
                                with st.form("form_entrada_lub", clear_on_submit=True):
                                    col1, col2 = st.columns(2)
                                    
                                    with col1:
                                        lubs = df_lub['nome'].tolist()
                                        lub_sel = st.selectbox("Lubrificante", lubs, key="entrada_lub")
                                        
                                        # Mostrar estoque atual do lubrificante selecionado
                                        if lub_sel:
                                            lub_info = df_lub[df_lub['nome'] == lub_sel].iloc[0]
                                            estoque_atual = lub_info['quantidade_estoque']
                                            unidade = lub_info['unidade']
                                            
                                            # Determinar cor do estoque
                                            if estoque_atual > 10:
                                                cor_estoque = "🟢"
                                            elif estoque_atual > 3:
                                                cor_estoque = "🟡"
                                            else:
                                                cor_estoque = "🔴"
                                            
                                            st.info(f"**Estoque atual:** {cor_estoque} {estoque_atual} {unidade}")
                                        
                                        quantidade = st.number_input("Quantidade a adicionar", min_value=0.01, step=0.5, format="%.2f", key="entrada_qtd")
                                    
                                    with col2:
                                        data_mov = st.date_input("Data da compra", value=date.today(), key="entrada_data")
                                        obs_mov = st.text_area("Observações (ex: fornecedor, nota fiscal)", placeholder="Ex: Compra da Petrobras, NF 123456", key="entrada_obs")
                                    
                                    submitted_entrada = st.form_submit_button("➕ Registrar Entrada", type="primary")
                                    if submitted_entrada:
                                        id_lub = df_lub[df_lub['nome'] == lub_sel]['id'].iloc[0]
                                        ok, msg = movimentar_lubrificante(id_lub, "entrada", quantidade, data_mov.strftime("%Y-%m-%d"), None, obs_mov)
                                        if ok:
                                            st.success(f"✅ {msg}")
                                            st.info(f"📦 **{quantidade} {unidade}** de '{lub_sel}' adicionados ao estoque.")
                                            st.rerun()
                                        else:
                                            st.error(f"❌ {msg}")
                            
                            # Aba de Saída (Consumo)
                            with tab_saida:
                                st.info("💡 **Saída:** Registre consumos manuais de lubrificantes (para casos não registrados via manutenção de componentes).")
                                
                                with st.form("form_saida_lub", clear_on_submit=True):
                                    col1, col2 = st.columns(2)
                                    
                                    with col1:
                                        lubs = df_lub['nome'].tolist()
                                        lub_sel = st.selectbox("Lubrificante", lubs, key="saida_lub")
                                        
                                        # Mostrar estoque atual do lubrificante selecionado
                                        if lub_sel:
                                            lub_info = df_lub[df_lub['nome'] == lub_sel].iloc[0]
                                            estoque_atual = lub_info['quantidade_estoque']
                                            unidade = lub_info['unidade']
                                            
                                            # Determinar cor do estoque
                                            if estoque_atual > 10:
                                                cor_estoque = "🟢"
                                            elif estoque_atual > 3:
                                                cor_estoque = "🟡"
                                            else:
                                                cor_estoque = "🔴"
                                            
                                            st.info(f"**Estoque atual:** {cor_estoque} {estoque_atual} {unidade}")
                                            
                                            # Aviso se estoque baixo
                                            if estoque_atual <= 3:
                                                st.warning(f"⚠️ **Estoque baixo!** Considere repor '{lub_sel}'")
                                        
                                        quantidade = st.number_input("Quantidade consumida", min_value=0.01, step=0.5, format="%.2f", max_value=estoque_atual if lub_sel else 999, key="saida_qtd")
                                    
                                    with col2:
                                        data_mov = st.date_input("Data do consumo", value=date.today(), key="saida_data")
                                        cod_equip = st.number_input("Código da Máquina (opcional)", min_value=0, step=1, key="saida_equip")
                                        obs_mov = st.text_area("Observações (ex: motivo do consumo)", placeholder="Ex: Consumo manual, vazamento, etc.", key="saida_obs")
                                    
                                    submitted_saida = st.form_submit_button("➖ Registrar Saída", type="secondary")
                                    if submitted_saida:
                                        id_lub = df_lub[df_lub['nome'] == lub_sel]['id'].iloc[0]
                                        ok, msg = movimentar_lubrificante(id_lub, "saida", quantidade, data_mov.strftime("%Y-%m-%d"), cod_equip if cod_equip > 0 else None, obs_mov)
                                        if ok:
                                            st.success(f"✅ {msg}")
                                            st.info(f"📉 **{quantidade} {unidade}** de '{lub_sel}' consumidos.")
                                            st.rerun()
                                        else:
                                            st.error(f"❌ {msg}")
                        else:
                            st.info("Cadastre lubrificantes para registrar movimentações.")

                        st.markdown("---")

                        # ----------- Histórico de Movimentações -----------
                        st.subheader("📊 Histórico de Movimentações")
                        
                        if not df_mov.empty:
                            df_mov['data'] = pd.to_datetime(df_mov['data'], errors='coerce')
                            df_mov = df_mov.sort_values('data', ascending=False)
                            # Junta nome do lubrificante
                            df_mov = df_mov.merge(df_lub[['id', 'nome', 'tipo', 'unidade']], left_on='id_lubrificante', right_on='id', how='left')
                            
                            # Criar abas para diferentes visualizações
                            tab_historico, tab_estatisticas = st.tabs(["📋 Histórico Detalhado", "📈 Estatísticas"])
                            
                            with tab_historico:
                                # Filtros
                                col_filtro1, col_filtro2, col_filtro3 = st.columns(3)
                                
                                with col_filtro1:
                                    lub_filtro = st.selectbox("Filtrar por lubrificante", ["Todos"] + df_lub['nome'].tolist())
                                
                                with col_filtro2:
                                    tipo_filtro = st.selectbox("Filtrar por tipo", ["Todos", "entrada", "saida"])
                                
                                with col_filtro3:
                                    data_inicio = st.date_input("Data início", value=df_mov['data'].min().date() if not df_mov.empty else date.today())
                                    data_fim = st.date_input("Data fim", value=df_mov['data'].max().date() if not df_mov.empty else date.today())
                                
                                # Aplicar filtros
                                df_filtrado = df_mov.copy()
                                
                                if lub_filtro != "Todos":
                                    df_filtrado = df_filtrado[df_filtrado['nome'] == lub_filtro]
                                
                                if tipo_filtro != "Todos":
                                    df_filtrado = df_filtrado[df_filtrado['tipo_x'] == tipo_filtro]
                                
                                df_filtrado = df_filtrado[
                                    (df_filtrado['data'].dt.date >= data_inicio) & 
                                    (df_filtrado['data'].dt.date <= data_fim)
                                ]
                                
                                # Preparar dados para exibição
                                df_mov_display = df_filtrado[['data', 'nome', 'tipo', 'tipo_x', 'quantidade', 'unidade', 'cod_equip', 'observacoes']].copy()
                                df_mov_display['data'] = df_mov_display['data'].dt.strftime('%d/%m/%Y')
                                df_mov_display['quantidade'] = df_mov_display['quantidade'].astype(str) + ' ' + df_mov_display['unidade']
                                df_mov_display = df_mov_display.rename(columns={
                                    'data': 'Data', 
                                    'nome': 'Lubrificante', 
                                    'tipo': 'Tipo Lubrificante', 
                                    'tipo_x': 'Movimentação', 
                                    'quantidade': 'Quantidade', 
                                    'cod_equip': 'Máquina', 
                                    'observacoes': 'Observações'
                                })
                                
                                # Remover coluna unidade que já foi concatenada
                                df_mov_display = df_mov_display.drop('unidade', axis=1)
                                
                                st.dataframe(df_mov_display, use_container_width=True)
                                
                                # Resumo dos filtros aplicados
                                st.info(f"📊 **Resumo:** {len(df_filtrado)} movimentações encontradas no período selecionado.")
                            
                            with tab_estatisticas:
                                # Estatísticas gerais
                                col_stat1, col_stat2, col_stat3 = st.columns(3)
                                
                                with col_stat1:
                                    total_entradas = len(df_mov[df_mov['tipo_x'] == 'entrada'])
                                    total_saidas = len(df_mov[df_mov['tipo_x'] == 'saida'])
                                    st.metric("Total Entradas", total_entradas)
                                    st.metric("Total Saídas", total_saidas)
                                
                                with col_stat2:
                                    qtd_entradas = df_mov[df_mov['tipo_x'] == 'entrada']['quantidade'].sum()
                                    qtd_saidas = df_mov[df_mov['tipo_x'] == 'saida']['quantidade'].sum()
                                    st.metric("Qtd. Total Entrada", f"{qtd_entradas:.1f}")
                                    st.metric("Qtd. Total Saída", f"{qtd_saidas:.1f}")
                                
                                with col_stat3:
                                    saldo = qtd_entradas - qtd_saidas
                                    st.metric("Saldo Geral", f"{saldo:.1f}")
                                
                                # Gráfico de movimentações por mês
                                if not df_mov.empty:
                                    df_mov['mes_ano'] = df_mov['data'].dt.to_period('M')
                                    mov_mensal = df_mov.groupby(['mes_ano', 'tipo_x'])['quantidade'].sum().reset_index()
                                    mov_mensal['mes_ano'] = mov_mensal['mes_ano'].astype(str)
                                    
                                    fig_mov = px.bar(
                                        mov_mensal,
                                        x='mes_ano',
                                        y='quantidade',
                                        color='tipo_x',
                                        title="Movimentações por Mês",
                                        labels={'mes_ano': 'Mês/Ano', 'quantidade': 'Quantidade', 'tipo_x': 'Tipo'},
                                        color_discrete_map={'entrada': 'green', 'saida': 'red'}
                                    )
                                    st.plotly_chart(fig_mov, use_container_width=True)
                        else:
                            st.info("Nenhuma movimentação registrada.")

                        st.markdown("---")

                        # ----------- Gestão de Almoxarifados -----------
                        st.subheader("🏢 Gestão de Almoxarifados")
                        
                        # Criar abas para cadastro e visualização
                        tab_cadastro_almox, tab_visualizar_almox = st.tabs(["➕ Cadastrar Almoxarifado", "📋 Almoxarifados Cadastrados"])
                        
                        # Aba de cadastro
                        with tab_cadastro_almox:
                            st.info("💡 **Cadastre os almoxarifados da empresa:** oficinas fixas e caminhões oficina móveis.")
                            
                            with st.form("form_add_almoxarifado", clear_on_submit=True):
                                col1, col2 = st.columns(2)
                                
                                with col1:
                                    nome_almox = st.text_input("Nome do Almoxarifado", placeholder="Ex: Oficina Central, Caminhão Oficina 01")
                                    tipo_almox = st.selectbox("Tipo", ["fixo", "movel"], 
                                                           format_func=lambda x: "🏢 Fixo (Oficina)" if x == "fixo" else "🚛 Móvel (Caminhão)")
                                    localizacao = st.text_input("Localização", placeholder="Ex: Sede da empresa, Região Sul")
                                
                                with col2:
                                    responsavel = st.text_input("Responsável", placeholder="Nome do responsável")
                                    observacoes = st.text_area("Observações", placeholder="Informações adicionais sobre o almoxarifado")
                                
                                submitted_almox = st.form_submit_button("➕ Cadastrar Almoxarifado", type="primary")
                                if submitted_almox:
                                    if nome_almox:
                                        success, message = add_almoxarifado(nome_almox, tipo_almox, localizacao, responsavel, observacoes)
                                        if success:
                                            st.success(f"✅ {message}")
                                            st.rerun()
                                        else:
                                            st.error(f"❌ {message}")
                                    else:
                                        st.warning("Por favor, informe o nome do almoxarifado.")
                        
                        # Aba de visualização
                        with tab_visualizar_almox:
                            if not df_almoxarifados.empty:
                                st.write("**Almoxarifados Cadastrados:**")
                                
                                for _, almox in df_almoxarifados.iterrows():
                                    with st.container():
                                        col1, col2, col3 = st.columns([3, 2, 1])
                                        
                                        with col1:
                                            if almox['tipo'] == 'fixo':
                                                st.write(f"🏢 **{almox['nome']}**")
                                            else:
                                                st.write(f"🚛 **{almox['nome']}**")
                                            
                                            if almox['localizacao']:
                                                st.write(f"📍 {almox['localizacao']}")
                                        
                                        with col2:
                                            if almox['responsavel']:
                                                st.write(f"👤 {almox['responsavel']}")
                                        
                                        with col3:
                                            if almox['tipo'] == 'fixo':
                                                st.write("🏢 Fixo")
                                            else:
                                                st.write("🚛 Móvel")
                                        
                                        if almox['observacoes']:
                                            st.write(f"📝 {almox['observacoes']}")
                                        
                                        st.markdown("---")
                                
                                # Estatísticas dos almoxarifados
                                st.subheader("📊 Estatísticas dos Almoxarifados")
                                
                                col_stat1, col_stat2, col_stat3 = st.columns(3)
                                
                                with col_stat1:
                                    total_almox = len(df_almoxarifados)
                                    st.metric("Total de Almoxarifados", total_almox)
                                
                                with col_stat2:
                                    almox_fixos = len(df_almoxarifados[df_almoxarifados['tipo'] == 'fixo'])
                                    st.metric("Almoxarifados Fixos", almox_fixos)
                                
                                with col_stat3:
                                    almox_moveis = len(df_almoxarifados[df_almoxarifados['tipo'] == 'movel'])
                                    st.metric("Almoxarifados Móveis", almox_moveis)
                                
                                # Gráfico de distribuição por tipo
                                fig_almox_tipo = px.pie(
                                    df_almoxarifados,
                                    names='tipo',
                                    title="Distribuição por Tipo de Almoxarifado",
                                    color_discrete_map={'fixo': 'blue', 'movel': 'orange'}
                                )
                                st.plotly_chart(fig_almox_tipo, use_container_width=True)
                                
                            else:
                                st.info("Nenhum almoxarifado cadastrado. Cadastre o primeiro almoxarifado na aba 'Cadastrar Almoxarifado'.")

                        conn.close()

            if tab_gerir_frotas is not None:
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
                            
                            # Campo de tipo de combustível
                            tipos_combustivel = ['Diesel S500', 'Diesel S10', 'Gasolina', 'Etanol', 'Biodiesel']
                            tipo_combustivel = st.selectbox("Tipo de Combustível", options=tipos_combustivel, index=0)
                            
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
                                        'ativo': ativo,
                                        'tipo_combustivel': tipo_combustivel
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
                                
                                # Campo de tipo de combustível
                                tipos_combustivel = ['Diesel S500', 'Diesel S10', 'Gasolina', 'Etanol', 'Biodiesel']
                                combustivel_atual = dados_atuais.get('tipo_combustivel', 'Diesel S500')
                                index_combustivel = tipos_combustivel.index(combustivel_atual) if combustivel_atual in tipos_combustivel else 0
                                novo_tipo_combustivel = st.selectbox("Tipo de Combustível", options=tipos_combustivel, index=index_combustivel)
                                
                                status_options = ["ATIVO", "INATIVO"]
                                index_status = status_options.index(dados_atuais['ATIVO']) if dados_atuais['ATIVO'] in status_options else 0
                                novo_status = st.selectbox("Status", options=status_options, index=index_status)
            
                                submitted = st.form_submit_button("Salvar Alterações na Frota")
                                if submitted:
                                    dados_editados = {
                                        'descricao': nova_descricao,
                                        'placa': nova_placa,
                                        'classe_op': nova_classe_op,
                                        'ativo': novo_status,
                                        'tipo_combustivel': novo_tipo_combustivel
                                    }
                                    if editar_frota(DB_PATH, cod_equip_edit, dados_editados):
                                        st.success("Dados da frota atualizados com sucesso!")
                                        rerun_keep_tab("⚙️ Gerir Frotas")
                
                        # NOVA SEÇÃO: Gerenciar Tipos de Combustível
                    st.markdown("---")
                    st.subheader("⛽ Gerenciar Tipos de Combustível")
                    st.info("Esta seção permite gerenciar os tipos de combustível das frotas de forma eficiente. Acesso restrito a administradores.")
                
                    # Criar abas para organizar as funcionalidades
                    tab_combustivel_classe, tab_combustivel_frota = st.tabs(["🔄 Por Classe", "✏️ Por Frota"])
                
                    with tab_combustivel_classe:
                        st.subheader("🔄 Aplicar Combustível a uma Classe Inteira")
                        st.write("Define o tipo de combustível para todas as frotas de uma classe específica. Útil para padronização em massa.")
                    
                        # Selecionar classe
                        classes_disponiveis = sorted([c for c in df_frotas['Classe_Operacional'].unique() if pd.notna(c) and str(c).strip()])
                    
                        if not classes_disponiveis:
                            st.warning("Nenhuma classe operacional encontrada. Verifique se há frotas cadastradas.")
                        else:
                            classe_selecionada = st.selectbox(
                                "Selecione a Classe:",
                                options=classes_disponiveis,
                                key="classe_combustivel_admin"
                            )
                        
                            # Mostrar informações sobre a classe selecionada
                            if classe_selecionada:
                                frotas_da_classe = df_frotas[df_frotas['Classe_Operacional'] == classe_selecionada]
                                st.info(f"**Classe selecionada:** {classe_selecionada}")
                                st.info(f"**Total de frotas:** {len(frotas_da_classe)}")
                                st.info(f"**Frotas ativas:** {len(frotas_da_classe[frotas_da_classe['ATIVO'] == 'ATIVO'])}")
                            
                                # Mostrar tipos de combustível atuais
                                if 'tipo_combustivel' in frotas_da_classe.columns:
                                    combustiveis_atuais = frotas_da_classe['tipo_combustivel'].value_counts()
                                    st.write("**Tipos de combustível atuais na classe:**")
                                    for combustivel, count in combustiveis_atuais.items():
                                        st.write(f"- {combustivel}: {count} frotas")
                            
                                # Selecionar novo tipo de combustível
                                tipos_combustivel = ['Diesel S500', 'Diesel S10', 'Gasolina', 'Etanol', 'Biodiesel']
                                tipo_combustivel_classe = st.selectbox(
                                    "Novo Tipo de Combustível:",
                                    options=tipos_combustivel,
                                    key="tipo_combustivel_classe_admin"
                                )
                            
                                if st.button("🔄 Aplicar à Classe Inteira", type="primary", use_container_width=True):
                                    with st.spinner("Aplicando tipo de combustível à classe..."):
                                        success, message = update_classe_combustivel(classe_selecionada, tipo_combustivel_classe)
                                        if success:
                                            st.success(message)
                                            # Limpar cache para atualizar dados
                                            st.cache_data.clear()
                                            st.rerun()
                                        else:
                                            st.error(message)
                
                    with tab_combustivel_frota:
                        st.subheader("✏️ Editar Combustível de uma Frota Específica")
                        st.write("Define o tipo de combustível para uma frota específica. Útil para casos especiais ou exceções.")
                    
                        # Selecionar frota
                        frotas_disponiveis = df_frotas[df_frotas['ATIVO'] == 'ATIVO'].copy()
                    
                        if frotas_disponiveis.empty:
                            st.warning("Nenhuma frota ativa encontrada. Verifique se há frotas cadastradas e ativas.")
                        else:
                            frotas_disponiveis['label_combustivel'] = (
                                frotas_disponiveis['Cod_Equip'].astype(str) + " - " + 
                                frotas_disponiveis['DESCRICAO_EQUIPAMENTO'].fillna('') + 
                                " (" + frotas_disponiveis['PLACA'].fillna('Sem Placa') + ")"
                            )
                        
                            frota_selecionada = st.selectbox(
                                "Selecione a Frota:",
                                options=frotas_disponiveis['label_combustivel'].tolist(),
                                key="frota_combustivel_admin"
                            )
                        
                            if frota_selecionada:
                                # Obter código da frota selecionada
                                cod_equip_frota = int(frota_selecionada.split(" - ")[0])
                            
                                # Obter dados da frota
                                dados_frota = frotas_disponiveis[frotas_disponiveis['Cod_Equip'] == cod_equip_frota].iloc[0]
                            
                                # Mostrar informações da frota
                                col_info1, col_info2 = st.columns(2)
                                with col_info1:
                                    st.write(f"**Código:** {dados_frota['Cod_Equip']}")
                                    st.write(f"**Descrição:** {dados_frota['DESCRICAO_EQUIPAMENTO']}")
                                with col_info2:
                                    st.write(f"**Placa:** {dados_frota['PLACA']}")
                                    st.write(f"**Classe:** {dados_frota['Classe_Operacional']}")
                            
                                # Verificar combustível atual
                                if 'tipo_combustivel' in dados_frota:
                                    combustivel_atual = dados_frota['tipo_combustivel']
                                    combustivel_atual = combustivel_atual if pd.notna(combustivel_atual) else 'Diesel S500'
                                else:
                                    combustivel_atual = 'Diesel S500'
                            
                                st.info(f"**Combustível atual:** {combustivel_atual}")
                            
                                # Selecionar novo tipo de combustível
                                tipos_combustivel = ['Diesel S500', 'Diesel S10', 'Gasolina', 'Etanol', 'Biodiesel']
                                novo_tipo_combustivel = st.selectbox(
                                    "Novo Tipo de Combustível:",
                                    options=tipos_combustivel,
                                    index=tipos_combustivel.index(combustivel_atual) if combustivel_atual in tipos_combustivel else 0,
                                    key="novo_tipo_combustivel_admin"
                                )
                            
                                if st.button("✏️ Atualizar Frota", type="secondary", use_container_width=True):
                                    with st.spinner("Atualizando tipo de combustível..."):
                                        success, message = update_frota_combustivel(cod_equip_frota, novo_tipo_combustivel)
                                        if success:
                                            st.success(message)
                                            # Limpar cache para atualizar dados
                                            st.cache_data.clear()
                                            st.rerun()
                                        else:
                                            st.error(message)
                
                    # Resumo dos tipos de combustível
                    st.markdown("---")
                    st.subheader("📊 Resumo dos Tipos de Combustível")
                
                    if 'tipo_combustivel' in df_frotas.columns:
                        # Estatísticas gerais
                        col_stats1, col_stats2, col_stats3 = st.columns(3)
                    
                        with col_stats1:
                            total_frotas = len(df_frotas)
                            st.metric("Total de Frotas", total_frotas)
                    
                        with col_stats2:
                            frotas_com_combustivel = df_frotas['tipo_combustivel'].notna().sum()
                            st.metric("Frotas com Combustível", frotas_com_combustivel)
                    
                        with col_stats3:
                            tipos_unicos = df_frotas['tipo_combustivel'].nunique()
                            st.metric("Tipos de Combustível", tipos_unicos)
                    
                        # Distribuição por tipo de combustível
                        st.write("**Distribuição por Tipo de Combustível:**")
                        combustivel_dist = df_frotas['tipo_combustivel'].value_counts()
                        for combustivel, count in combustivel_dist.items():
                            percentual = (count / total_frotas) * 100
                            st.write(f"- **{combustivel}: {count} frotas ({percentual:.1f}%)")
                    else:
                        st.warning("Coluna de tipo de combustível não encontrada. Execute a aplicação para criar automaticamente.")

                # APAGUE O CONTEÚDO DA SUA "with tab_config:" E SUBSTITUA-O POR ESTE BLOCO

        if tab_config is not None:
            with tab_config:
                st.header("⚙️ Configurar Manutenções e Checklists")
                
                # Informações sobre as novas funcionalidades
                with st.expander("ℹ️ Sobre as Novas Funcionalidades de Componentes e Lubrificantes", expanded=False):
                    st.info("""
                    **🆕 Novas Funcionalidades Implementadas:**
                    
                    **1. Integração Componentes-Lubrificantes:**
                    - Agora você pode associar lubrificantes específicos aos componentes
                    - Defina intervalos de troca personalizados para cada componente
                    - Escolha o tipo de manutenção: Troca, Remonta ou Ambos
                    
                    **2. Registro Avançado de Manutenção:**
                    - Registre se foi uma troca ou remonta
                    - Sistema automaticamente atualiza o estoque de lubrificantes
                    - Histórico detalhado com tipo de serviço e lubrificante utilizado
                    
                    **3. Indicadores na Ficha Individual:**
                    - Status atual de cada componente (km/horas restantes)
                    - Contagem separada de trocas vs remontas
                    - Informações do lubrificante associado
                    - Gráficos de distribuição por tipo de manutenção
                    
                    **4. Gestão de Estoque Automática:**
                    - Estoque de lubrificantes é atualizado automaticamente
                    - Controle de entrada e saída integrado
                    """)
                
                # --- Gestão de Componentes e Lubrificantes ---
                exp_comp_open = st.session_state.get('open_expander_config_componentes', False)
                with st.expander("Configurar Componentes e Lubrificantes por Classe", expanded=bool(exp_comp_open)):
                    # Garantir que a tabela de lubrificantes existe
                    ensure_lubrificantes_schema()
                    
                    classes_operacionais = sorted([c for c in df_frotas['Classe_Operacional'].unique() if pd.notna(c) and str(c).strip()])
                    df_comp_regras = get_component_rules() # Busca os dados mais recentes
                    
                    # Carregar lubrificantes disponíveis
                    try:
                        conn = sqlite3.connect(DB_PATH)
                        df_lubrificantes = pd.read_sql("SELECT id, nome, tipo, viscosidade FROM lubrificantes ORDER BY nome", conn)
                        conn.close()
                    except Exception as e:
                        # Em caso de erro, criar DataFrame vazio
                        df_lubrificantes = pd.DataFrame(columns=['id', 'nome', 'tipo', 'viscosidade'])
                        st.warning("⚠️ Tabela de lubrificantes não encontrada. Cadastre lubrificantes na aba 'Gestão de Lubrificantes'.")

                    # Selecionar classe para configurar
                    classe_selecionada = st.selectbox(
                        "Selecione a Classe Operacional para configurar:",
                        options=classes_operacionais,
                        key="classe_config_select"
                    )
                    
                    if classe_selecionada:
                        st.subheader(f"Configuração da Classe: {classe_selecionada}")
                        
                        # Mostrar componentes existentes em uma tabela compacta
                        regras_atuais = df_comp_regras[df_comp_regras['classe_operacional'] == classe_selecionada]
                        
                        if not regras_atuais.empty:
                            st.write("**Componentes Configurados:**")
                            
                            # Criar DataFrame para exibição
                            df_display = regras_atuais.copy()
                            
                            # Verificar se a coluna lubrificante_id existe
                            if 'lubrificante_id' in df_display.columns:
                                df_display['Lubrificante'] = df_display['lubrificante_id'].apply(
                                    lambda x: df_lubrificantes[df_lubrificantes['id'] == x]['nome'].iloc[0] if x and not df_lubrificantes[df_lubrificantes['id'] == x].empty else "Sem lubrificante"
                                )
                            else:
                                # Se a coluna não existe, criar coluna vazia
                                df_display['lubrificante_id'] = None
                                df_display['Lubrificante'] = "Sem lubrificante"
                            
                            df_display['Unidade'] = 'km' if df_frotas[df_frotas['Classe_Operacional'] == classe_selecionada]['Tipo_Controle'].iloc[0] == 'QUILÔMETROS' else 'h'
                            df_display['Intervalo'] = df_display['intervalo_padrao'].astype(str) + ' ' + df_display['Unidade']
                            
                            # Exibir tabela com ações
                            for _, regra in regras_atuais.iterrows():
                                with st.container():
                                    col1, col2, col3, col4, col5 = st.columns([3, 1, 1, 1, 1])
                                    
                                    with col1:
                                        st.write(f"**{regra['nome_componente']}**")
                                    
                                    with col2:
                                        st.write(f"{regra['intervalo_padrao']} {df_display['Unidade'].iloc[0]}")
                                    
                                    with col3:
                                        if 'Lubrificante' in df_display.columns:
                                            lub_info = df_display[df_display['nome_componente'] == regra['nome_componente']]['Lubrificante'].iloc[0]
                                            if lub_info != "Sem lubrificante":
                                                # Buscar estoque do lubrificante
                                                try:
                                                    conn = sqlite3.connect(DB_PATH)
                                                    df_lub_estoque = pd.read_sql(
                                                        "SELECT quantidade_estoque, unidade FROM lubrificantes WHERE nome = ?", 
                                                        conn, params=(lub_info,)
                                                    )
                                                    conn.close()
                                                    
                                                    if not df_lub_estoque.empty:
                                                        estoque = df_lub_estoque.iloc[0]['quantidade_estoque']
                                                        unidade = df_lub_estoque.iloc[0]['unidade']
                                                        
                                                        # Determinar cor do estoque
                                                        if estoque > 10:
                                                            cor_estoque = "🟢"
                                                        elif estoque > 3:
                                                            cor_estoque = "🟡"
                                                        else:
                                                            cor_estoque = "🔴"
                                                        
                                                        st.write(f"{lub_info}")
                                                        st.write(f"{cor_estoque} {estoque} {unidade}")
                                                    else:
                                                        st.write(lub_info)
                                                except:
                                                    st.write(lub_info)
                                            else:
                                                st.write("Sem lubrificante")
                                        else:
                                            st.write("Sem lubrificante")
                                    
                                    with col4:
                                        tipo_manut = regra.get('tipo_manutencao', 'Troca') if 'tipo_manutencao' in regra else 'Troca'
                                        st.write(tipo_manut)
                                    
                                    with col5:
                                        # Botões de ação
                                        col_edit, col_del = st.columns(2)
                                        with col_edit:
                                            if st.button("✏️", key=f"edit_comp_{regra['id_regra']}", help="Editar componente"):
                                                st.session_state['editing_component'] = regra['id_regra']
                                                st.session_state['editing_component_data'] = regra.to_dict()
                                                rerun_keep_tab("⚙️ Configurações")
                                        with col_del:
                                            if st.button("🗑️", key=f"del_comp_{regra['id_regra']}", help="Remover componente"):
                                                delete_component_rule(regra['id_regra'])
                                                rerun_keep_tab("⚙️ Configurações")
                                    
                                    st.markdown("---")
                        else:
                            st.info("Nenhum componente configurado para esta classe.")
                        
                        st.markdown("---")
                        
                        # Verificar se está editando um componente
                        editing_component = st.session_state.get('editing_component')
                        editing_data = st.session_state.get('editing_component_data')
                        
                        if editing_component and editing_data:
                            # Formulário de edição
                            with st.form(f"form_edit_{classe_selecionada}", clear_on_submit=True):
                                st.write("**✏️ Editar Componente**")
                                
                                col1, col2 = st.columns(2)
                                
                                with col1:
                                    novo_comp_nome = st.text_input("Nome do Componente", value=editing_data['nome_componente'], key=f"edit_nome_{classe_selecionada}")
                                    novo_comp_intervalo = st.number_input("Intervalo de Troca (km/h)", min_value=1, step=50, value=editing_data['intervalo_padrao'], key=f"edit_int_{classe_selecionada}")
                                
                                with col2:
                                    # Seleção de lubrificante (opcional)
                                    if not df_lubrificantes.empty:
                                        lubrificantes_opcoes = ["Sem lubrificante"] + df_lubrificantes['nome'].tolist()
                                        lub_atual = "Sem lubrificante"
                                        if 'lubrificante_id' in editing_data and editing_data.get('lubrificante_id'):
                                            lub_info = df_lubrificantes[df_lubrificantes['id'] == editing_data['lubrificante_id']]
                                            if not lub_info.empty:
                                                lub_atual = lub_info.iloc[0]['nome']
                                        
                                        index_lub = lubrificantes_opcoes.index(lub_atual) if lub_atual in lubrificantes_opcoes else 0
                                        lubrificante_selecionado = st.selectbox("Lubrificante (opcional)", options=lubrificantes_opcoes, index=index_lub, key=f"edit_lub_{classe_selecionada}")
                                    else:
                                        lubrificante_selecionado = st.selectbox("Lubrificante (opcional)", options=["Sem lubrificante"], key=f"edit_lub_{classe_selecionada}")
                                        st.info("💡 Cadastre lubrificantes na aba 'Gestão de Lubrificantes' para associá-los aos componentes.")
                                    
                                    # Tipo de manutenção
                                    tipo_atual = editing_data.get('tipo_manutencao', 'Troca') if 'tipo_manutencao' in editing_data else 'Troca'
                                    tipo_opcoes = ["Troca", "Remonta", "Ambos"]
                                    index_tipo = tipo_opcoes.index(tipo_atual) if tipo_atual in tipo_opcoes else 0
                                    tipo_manutencao = st.selectbox("Tipo de Manutenção", options=tipo_opcoes, index=index_tipo, key=f"edit_tipo_{classe_selecionada}")
                                
                                col_save, col_cancel = st.columns(2)
                                with col_save:
                                    if st.form_submit_button("💾 Salvar Alterações"):
                                        if novo_comp_nome:
                                            # Obter ID do lubrificante se selecionado
                                            lubrificante_id = None
                                            if lubrificante_selecionado != "Sem lubrificante":
                                                lub_info = df_lubrificantes[df_lubrificantes['nome'] == lubrificante_selecionado]
                                                if not lub_info.empty:
                                                    lubrificante_id = lub_info.iloc[0]['id']
                                            
                                            success, message = update_component_rule(editing_component, novo_comp_nome, novo_comp_intervalo, lubrificante_id, tipo_manutencao)
                                            if success:
                                                st.success(message)
                                                # Limpar estado de edição
                                                st.session_state.pop('editing_component', None)
                                                st.session_state.pop('editing_component_data', None)
                                                rerun_keep_tab("⚙️ Configurações")
                                            else:
                                                st.error(message)
                                        else:
                                            st.warning("Por favor, informe o nome do componente.")
                                
                                with col_cancel:
                                    if st.form_submit_button("❌ Cancelar"):
                                        st.session_state.pop('editing_component', None)
                                        st.session_state.pop('editing_component_data', None)
                                        rerun_keep_tab("⚙️ Configurações")
                        
                        # Formulário para adicionar novo componente
                        with st.form(f"form_add_{classe_selecionada}", clear_on_submit=True):
                            st.write("**➕ Adicionar Novo Componente**")
                            
                            col1, col2 = st.columns(2)
                            
                            with col1:
                                novo_comp_nome = st.text_input("Nome do Componente", key=f"nome_{classe_selecionada}")
                                novo_comp_intervalo = st.number_input("Intervalo de Troca (km/h)", min_value=1, step=50, key=f"int_{classe_selecionada}")
                                novo_comp_capacidade = st.number_input("Capacidade (litros)", min_value=0.0, step=0.5, format="%.1f", key=f"cap_{classe_selecionada}")
                            
                            with col2:
                                    # Seleção de lubrificante (opcional)
                                if not df_lubrificantes.empty:
                                    lubrificantes_opcoes = ["Sem lubrificante"] + df_lubrificantes['nome'].tolist()
                                    lubrificante_selecionado = st.selectbox("Lubrificante (opcional)", options=lubrificantes_opcoes, key=f"lub_{classe_selecionada}")
                                else:
                                    lubrificante_selecionado = st.selectbox("Lubrificante (opcional)", options=["Sem lubrificante"], key=f"lub_{classe_selecionada}")
                                    st.info("💡 Cadastre lubrificantes na aba 'Gestão de Lubrificantes' para associá-los aos componentes.")
                                    
                                    # Tipo de manutenção
                                tipo_manutencao = st.selectbox("Tipo de Manutenção", options=["Troca", "Remonta", "Ambos"], key=f"tipo_{classe_selecionada}")
                            
                            # Informações sobre tipos de manutenção
                            with st.expander("💡 Sobre Tipos de Manutenção", expanded=False):
                                st.info("""
                                **🔄 Troca vs Remonta:**
                                
                                **Troca:**
                                - Reinicia o contador de km/horas
                                - Exemplos: troca de óleo, filtros, correias
                                - Usado para manutenções que "zeram" o ciclo
                                
                                **Remonta:**
                                - NÃO reinicia o contador de km/horas
                                - Exemplos: ajuste, limpeza, reparo
                                - Usado para manutenções complementares
                                
                                **Ambos:**
                                - Permite escolher entre troca ou remonta
                                - Flexível para componentes que podem ter ambos os tipos
                                
                                **⚠️ Importante:** Apenas TROCAS reiniciam o contador para calcular próxima manutenção!
                                """)
                            
                            if st.form_submit_button("➕ Adicionar Componente"):
                                if novo_comp_nome:
                                    # Obter ID do lubrificante se selecionado
                                    lubrificante_id = None
                                    if lubrificante_selecionado != "Sem lubrificante":
                                        lub_info = df_lubrificantes[df_lubrificantes['nome'] == lubrificante_selecionado]
                                        if not lub_info.empty:
                                            lubrificante_id = lub_info.iloc[0]['id']
                                    
                                    success, message = add_component_rule_advanced(classe_selecionada, novo_comp_nome, novo_comp_intervalo, lubrificante_id, tipo_manutencao, novo_comp_capacidade)
                                    if success:
                                        st.success(message)
                                        st.session_state['open_expander_config_componentes'] = True
                                        rerun_keep_tab("⚙️ Configurações")
                                    else:
                                        st.error(message)
                                else:
                                    st.warning("Por favor, informe o nome do componente.")

                # --- Gestão de Checklists ---
                exp_chk_open = st.session_state.get('open_expander_config_checklists', True)
                with st.expander("Configurar Checklists Diários", expanded=bool(exp_chk_open)):
                    regras_checklist = get_checklist_rules()
                    
                    # Seção de checklists existentes
                    if not regras_checklist.empty:
                        st.subheader("📋 Modelos de Checklist Existentes")
                        
                        # Criar abas para cada checklist
                        for _, regra in regras_checklist.iterrows():
                            with st.container():
                                col1, col2, col3, col4, col5 = st.columns([3, 1, 1, 1, 1])
                                
                                with col1:
                                    st.write(f"**{regra['titulo_checklist']}**")
                                
                                with col2:
                                    st.write(regra['classe_operacional'])
                                
                                with col3:
                                    st.write(regra['frequencia'])
                                
                                with col4:
                                    st.write(regra['turno'])
                                
                                with col5:
                                    # Botões de ação
                                    col_edit_chk, col_del_chk = st.columns(2)
                                    with col_edit_chk:
                                        if st.button("✏️", key=f"edit_chk_{regra['id_regra']}", help="Editar checklist"):
                                            st.session_state['editing_checklist'] = regra['id_regra']
                                            st.session_state['editing_checklist_data'] = regra.to_dict()
                                            rerun_keep_tab("⚙️ Configurações")
                                    with col_del_chk:
                                        if st.button("🗑️", key=f"del_chk_{regra['id_regra']}", help="Remover checklist"):
                                            # Buscar e excluir itens primeiro
                                            itens_checklist = get_checklist_items(regra['id_regra'])
                                            for _, item in itens_checklist.iterrows():
                                                delete_checklist_item(item['id_item'])
                                            # Excluir regra
                                            delete_checklist_rule(regra['id_regra'])
                                            st.success("Checklist removido com sucesso!")
                                            rerun_keep_tab("⚙️ Configurações")
                                
                                # Mostrar itens do checklist
                                itens_checklist = get_checklist_items(regra['id_regra'])
                                if not itens_checklist.empty:
                                    with st.expander(f"Ver itens do checklist '{regra['titulo_checklist']}'", expanded=False):
                                        for _, item in itens_checklist.iterrows():
                                            col_item, col_del_item = st.columns([4, 1])
                                            with col_item:
                                                st.write(f"• {item['nome_item']}")
                                            with col_del_item:
                                                if st.button("🗑️", key=f"del_item_{item['id_item']}", help="Remover item"):
                                                    delete_checklist_item(item['id_item'])
                                                    st.success("Item removido!")
                                                    rerun_keep_tab("⚙️ Configurações")
                                
                                st.markdown("---")
                    else:
                        st.info("Nenhum modelo de checklist criado.")
                    
                    # Verificar se está editando um checklist
                    editing_checklist = st.session_state.get('editing_checklist')
                    editing_checklist_data = st.session_state.get('editing_checklist_data')
                    
                    if editing_checklist and editing_checklist_data:
                        # Formulário de edição de checklist
                        with st.form(f"form_edit_checklist", clear_on_submit=True):
                            st.subheader("✏️ Editar Checklist")
                            
                            col1_edit, col2_edit = st.columns(2)
                            
                            with col1_edit:
                                novo_titulo = st.text_input("Título do Checklist", value=editing_checklist_data['titulo_checklist'], key="edit_chk_titulo")
                                nova_frequencia = st.selectbox("Frequência", options=['Diário', 'Dias Pares', 'Dias Ímpares'], 
                                                             index=['Diário', 'Dias Pares', 'Dias Ímpares'].index(editing_checklist_data['frequencia']), 
                                                             key="edit_chk_freq")
                            
                            with col2_edit:
                                nova_classe = st.selectbox("Classe Operacional", options=classes_operacionais, 
                                                         index=classes_operacionais.index(editing_checklist_data['classe_operacional']), 
                                                         key="edit_chk_classe")
                                novo_turno = st.selectbox("Turno", options=['Manhã', 'Noite', 'N/A'], 
                                                        index=['Manhã', 'Noite', 'N/A'].index(editing_checklist_data['turno']), 
                                                        key="edit_chk_turno")
                            
                            # Mostrar itens atuais
                            itens_atuais = get_checklist_items(editing_checklist)
                            if not itens_atuais.empty:
                                st.write("**Itens atuais:**")
                                for _, item in itens_atuais.iterrows():
                                    st.write(f"• {item['nome_item']}")
                            
                            st.write("**Novos itens (substituirão os atuais):**")
                            novos_itens_texto = st.text_area("Itens do Checklist", height=150, key="edit_chk_itens", 
                                                           placeholder="Nível do Óleo\nPressão dos Pneus\nVerificar Facas")
                            
                            col_save_chk, col_cancel_chk = st.columns(2)
                            with col_save_chk:
                                if st.form_submit_button("💾 Salvar Alterações"):
                                    if novo_titulo and novos_itens_texto:
                                        # Atualizar regra
                                        success, msg = edit_checklist_rule(editing_checklist, editing_checklist_data['classe_operacional'], 
                                                                         novo_titulo, editing_checklist_data['turno'], nova_frequencia)
                                        if success:
                                            # Remover itens antigos
                                            for _, item in itens_atuais.iterrows():
                                                delete_checklist_item(item['id_item'])
                                            
                                            # Adicionar novos itens
                                            itens_lista = [item.strip() for item in novos_itens_texto.split('\n') if item.strip()]
                                            for item in itens_lista:
                                                add_checklist_item(editing_checklist, item)
                                            
                                            st.success("Checklist atualizado com sucesso!")
                                            st.session_state.pop('editing_checklist', None)
                                            st.session_state.pop('editing_checklist_data', None)
                                            rerun_keep_tab("⚙️ Configurações")
                                        else:
                                            st.error(msg)
                                    else:
                                        st.warning("Por favor, preencha todos os campos obrigatórios.")
                            
                            with col_cancel_chk:
                                if st.form_submit_button("❌ Cancelar"):
                                    st.session_state.pop('editing_checklist', None)
                                    st.session_state.pop('editing_checklist_data', None)
                                    rerun_keep_tab("⚙️ Configurações")
                    
                    # Formulário para adicionar novo checklist
                    with st.form("form_add_checklist", clear_on_submit=True):
                        st.subheader("➕ Criar Novo Modelo de Checklist")
                        
                        col1_form, col2_form = st.columns(2)
                        nova_classe = col1_form.selectbox("Aplicar à Classe Operacional", options=classes_operacionais, key="chk_classe")
                        novo_titulo = col1_form.text_input("Título do Checklist (ex: Verificação Matinal Colhedoras)", key="chk_titulo")
                        nova_frequencia = col2_form.selectbox("Frequência", options=['Diário', 'Dias Pares', 'Dias Ímpares'], key="chk_freq")
                        novo_turno = col2_form.selectbox("Turno", options=['Manhã', 'Noite', 'N/A'], key="chk_turno")
                        
                        st.write("**Itens a serem verificados (um por linha):**")
                        novos_itens_texto = st.text_area("Itens do Checklist", height=150, key="chk_itens", placeholder="Nível do Óleo\nPressão dos Pneus\nVerificar Facas")
                        
                        if st.form_submit_button("➕ Salvar Novo Modelo de Checklist"):
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
                        
        if tab_importar is not None:
            with tab_importar:
                st.header("📤 Importar Dados")
                sub_tab_abastec, sub_tab_motoristas, sub_tab_precos, sub_tab_pneus, sub_tab_lubrificantes, sub_tab_componentes = st.tabs(
                    ["⛽ Abastecimentos", "👤 Motoristas", "💲 Preços de Combustível", "🚚 Pneus", "🛢️ Lubrificantes", "⚙️ Componentes"]
                )

                with sub_tab_abastec:
                    st.subheader("Importar Novos Abastecimentos de uma Planilha")
                    st.info("Carregue múltiplos abastecimentos de uma vez (Excel .xlsx). Colunas: `Cód. Equip.`, `Data`, `Qtde Litros`, `Hod. Hor. Atual`, `Safra`, `Mês`, `Classe Operacional`, opcional `Matricula`, `Cod_Pessoa`.")
                    arquivo_carregado = st.file_uploader(
                        "Selecione a sua planilha de abastecimentos",
                        type=['xlsx'], key="upl_abast"
                    )
                    if arquivo_carregado is not None:
                        st.markdown("---")
                        st.write("Pré-visualização:")
                        try:
                            df_preview = pd.read_excel(arquivo_carregado)
                            st.dataframe(df_preview.head())
                            if st.button("Confirmar e Inserir Dados", type="primary"):
                                with st.spinner("Importando dados..."):
                                    num_inseridos, num_duplicados, mensagem = importar_abastecimentos_de_planilha(DB_PATH, arquivo_carregado)
                                if num_inseridos > 0:
                                    msg_sucesso = f"{num_inseridos} registos importados."
                                    if num_duplicados > 0:
                                        msg_sucesso += f" {num_duplicados} duplicados ignorados."
                                    st.success(msg_sucesso)
                                    rerun_keep_tab("📤 Importar Dados")
                                else:
                                    st.error(mensagem)
                        except Exception as e:
                            st.error(f"Não foi possível ler a planilha: {e}")

                with sub_tab_motoristas:
                    st.subheader("Importar Motoristas por Planilha")
                    st.info("Colunas esperadas: `Matricula`, `Nome`, opcional `Cod_Pessoa`. A matrícula será exibida nos gráficos; na consulta será mostrada matrícula e nome.")
                    arquivo_motoristas = st.file_uploader("Selecione a planilha de motoristas", type=['xlsx'], key="upl_motoristas")
                    if arquivo_motoristas is not None:
                            try:
                                df_prev = pd.read_excel(arquivo_motoristas)
                                st.dataframe(df_prev.head())
                                if st.button("Confirmar e Inserir Motoristas", type="primary"):
                                    with st.spinner("Importando motoristas..."):
                                        ensure_motoristas_schema()
                                        inseridos, duplicados, msg = importar_motoristas_de_planilha(DB_PATH, arquivo_motoristas)
                                    if inseridos > 0:
                                        st.success(f"{msg}")
                                        rerun_keep_tab("📤 Importar Dados")
                                    else:
                                        st.error(msg)
                            except Exception as e:
                                st.error(f"Erro ao ler planilha: {e}")

                with sub_tab_precos:
                    st.subheader("Definir Preços por Tipo de Combustível")
                    ok, msg = ensure_precos_combustivel_schema()
                    if not ok:
                        st.warning(msg)
                        precos_map = get_precos_combustivel_map()
                        tipos = ['Diesel S500', 'Diesel S10', 'Gasolina', 'Etanol', 'Biodiesel']
                        cols = st.columns(5)
                        novos_precos = {}
                        for i, t in enumerate(tipos):
                            with cols[i % 5]:
                                valor = st.number_input(f"{t}", min_value=0.0, format="%.3f", value=float(precos_map.get(t) or 0.0), key=f"preco_{t}")
                                novos_precos[t] = valor
                        if st.button("Salvar Preços", type="secondary"):
                            with st.spinner("Salvando preços..."):
                                ok_all = True
                                for t, p in novos_precos.items():
                                    ok, _ = upsert_preco_combustivel(t, float(p) if p is not None else None)
                                    ok_all = ok_all and ok
                                if ok_all:
                                    st.success("Preços atualizados.")
                                else:
                                    st.warning("Alguns preços podem não ter sido salvos.")
                                    
                with sub_tab_pneus:
                    st.subheader("Importar Histórico de Pneus")
                    st.info(
                            "Colunas obrigatórias na planilha: `Cod_Equip`, `posicao`, `marca`, `modelo`, `data_instalacao`, `hodometro_instalacao`, `vida_util_km`. Opcional: `observacoes`.\n"
                            "Cada pneu será vinculado à frota pelo campo `Cod_Equip`."
                        )
                    arquivo_pneus = st.file_uploader("Selecione a planilha de pneus", type=['xlsx'], key="upl_pneus")
                    if arquivo_pneus is not None:
                            try:
                                df_prev = pd.read_excel(arquivo_pneus)
                                st.dataframe(df_prev.head())
                                if st.button("Confirmar e Inserir Pneus", type="primary"):
                                    ensure_pneus_schema()
                                    inseridos, duplicados, msg = importar_pneus_de_planilha(DB_PATH, arquivo_pneus)
                                    if inseridos > 0:
                                        st.success(f"{msg}")
                                        st.rerun()
                                    else:
                                        st.error(msg)
                            except Exception as e:
                                st.error(f"Erro ao ler planilha: {e}")

                    st.markdown("---")
                    st.subheader("Cadastrar Pneus Manualmente")
                    with st.form("form_add_pneu", clear_on_submit=True):
                            cod_equip = st.number_input("Código da Frota", min_value=1, step=1)
                            posicao = st.text_input("Posição do Pneu (ex: Dianteiro Esquerdo)")
                            marca = st.text_input("Marca")
                            modelo = st.text_input("Modelo")
                            data_instalacao = st.date_input("Data de Instalação")
                            hodometro_instalacao = st.number_input("Leitura na Instalação", min_value=0.0, format="%.2f")
                            vida_util_km = st.number_input("Vida Útil Estimada (km)", min_value=0.0, format="%.2f")
                            observacoes = st.text_area("Observações", height=50)
                            status = st.selectbox("Status do Pneu", ["Ativo", "Sucateado", "Reformado"])
                            vida_atual = st.number_input("Vida Atual do Pneu", min_value=1, step=1, value=1)
                            if st.form_submit_button("Salvar Pneu"):
                                ensure_pneus_schema()
                                try:
                                    with sqlite3.connect(DB_PATH, check_same_thread=False) as conn:
                                        cur = conn.cursor()
                                        cur.execute(
                                            "INSERT INTO pneus_historico (Cod_Equip, posicao, marca, modelo, data_instalacao, hodometro_instalacao, vida_util_km, observacoes, status, vida_atual) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                                            (cod_equip, posicao, marca, modelo, data_instalacao.strftime("%Y-%m-%d"), hodometro_instalacao, vida_util_km, observacoes, status, vida_atual)
                                        )
                                        conn.commit()
                                    st.success("Pneu cadastrado com sucesso!")
                                    st.rerun()
                                except Exception as e:
                                    st.error(f"Erro ao cadastrar pneu: {e}")

                    st.markdown("---")
                    st.subheader("Consultar Histórico de Pneus por Frota")
                    cod_equip_pneu = st.number_input("Código da Frota para consulta", min_value=1, step=1)
                    filtro_marca = st.text_input("Filtrar por Marca (opcional)")
                    filtro_posicao = st.text_input("Filtrar por Posição (opcional)")
                    if cod_equip_pneu:
                            df_hist_pneus = get_pneus_historico(cod_equip_pneu)
                            if filtro_marca:
                                df_hist_pneus = df_hist_pneus[df_hist_pneus['marca'].str.contains(filtro_marca, case=False, na=False)]
                            if filtro_posicao:
                                df_hist_pneus = df_hist_pneus[df_hist_pneus['posicao'].str.contains(filtro_posicao, case=False, na=False)]
                            if not df_hist_pneus.empty:
                                st.dataframe(df_hist_pneus)
                                # Edição e exclusão
                                st.markdown("### Editar ou Excluir Pneus")
                                df_hist_pneus['label'] = (
                                    df_hist_pneus['id'].astype(str) + " | " +
                                    df_hist_pneus['posicao'] + " | " +
                                    df_hist_pneus['marca'] + " | " +
                                    df_hist_pneus['modelo']
                                )
                                pneu_sel = st.selectbox("Selecione o pneu para editar/excluir", options=df_hist_pneus['label'])
                                if pneu_sel:
                                    pneu_row = df_hist_pneus[df_hist_pneus['label'] == pneu_sel].iloc[0]
                                    with st.expander("Editar Pneu"):
                                        with st.form("form_edit_pneu", clear_on_submit=True):
                                            nova_posicao = st.text_input("Posição", value=pneu_row['posicao'])
                                            nova_marca = st.text_input("Marca", value=pneu_row['marca'])
                                            novo_modelo = st.text_input("Modelo", value=pneu_row['modelo'])
                                            nova_data = st.date_input("Data de Instalação", value=pd.to_datetime(pneu_row['data_instalacao']))
                                            novo_hod = st.number_input("Leitura na Instalação", value=float(pneu_row['hodometro_instalacao']), format="%.2f")
                                            nova_vida = st.number_input("Vida Útil Estimada (km)", value=float(pneu_row['vida_util_km']), format="%.2f")
                                            novas_obs = st.text_area("Observações", value=pneu_row['observacoes'], height=50)
                                            if st.form_submit_button("Salvar Alterações"):
                                                try:
                                                    with sqlite3.connect(DB_PATH, check_same_thread=False) as conn:
                                                        cur = conn.cursor()
                                                        cur.execute(
                                                            "UPDATE pneus_historico SET posicao=?, marca=?, modelo=?, data_instalacao=?, hodometro_instalacao=?, vida_util_km=?, observacoes=? WHERE id=?",
                                                            (nova_posicao, nova_marca, novo_modelo, nova_data.strftime("%Y-%m-%d"), novo_hod, nova_vida, novas_obs, pneu_row['id'])
                                                        )
                                                        conn.commit()
                                                    st.success("Pneu atualizado com sucesso!")
                                                    st.rerun()
                                                except Exception as e:
                                                    st.error(f"Erro ao editar pneu: {e}")
                                    if st.button("Excluir Pneu Selecionado", type="primary"):
                                        try:
                                            with sqlite3.connect(DB_PATH, check_same_thread=False) as conn:
                                                cur = conn.cursor()
                                                cur.execute("DELETE FROM pneus_historico WHERE id=?", (pneu_row['id'],))
                                                conn.commit()
                                            st.success("Pneu excluído com sucesso!")
                                            st.rerun()
                                        except Exception as e:
                                            st.error(f"Erro ao excluir pneu: {e}")
                            else:
                                st.info("Nenhum registro de pneus para esta frota.")
                with sub_tab_lubrificantes:
                        st.subheader("Importar Lubrificantes por Planilha")
                        st.info("Colunas obrigatórias: nome, tipo (graxa/óleo), viscosidade, quantidade_estoque, unidade, observacoes")
                        arquivo_lub = st.file_uploader("Selecione a planilha de lubrificantes", type=['xlsx'], key="upl_lub")
                        if arquivo_lub is not None:
                            try:
                                df_lub_import = pd.read_excel(arquivo_lub)
                                st.dataframe(df_lub_import.head())
                                if st.button("Confirmar e Inserir Lubrificantes", type="primary"):
                                    with st.spinner("Importando lubrificantes..."):
                                        inseridos, duplicados, msg = importar_lubrificantes_de_planilha(DB_PATH, arquivo_lub)
                                    if inseridos > 0:
                                        st.success(f"{msg}")
                                        st.rerun()
                                    else:
                                        st.error(msg)
                            except Exception as e:
                                st.error(f"Erro ao importar lubrificantes: {e}")

                        st.markdown("---")
                        st.subheader("Cadastrar Lubrificante Manualmente")
                        with st.form("form_add_lub", clear_on_submit=True):
                            nome = st.text_input("Nome")
                            tipo = st.selectbox("Tipo", ["óleo", "graxa"])
                            viscosidade = st.text_input("Viscosidade")
                            quantidade = st.number_input("Quantidade Inicial", min_value=0.0, format="%.2f")
                            unidade = st.selectbox("Unidade", ["L", "kg", "gal"])
                            obs = st.text_area("Observações")
                            if st.form_submit_button("Salvar Lubrificante"):
                                with sqlite3.connect(DB_PATH) as conn:
                                    cur = conn.cursor()
                                    cur.execute("PRAGMA table_info(lubrificantes)")
                                    cols = [c[1] for c in cur.fetchall()]
                                    if 'tipo' not in cols:
                                        cur.execute("ALTER TABLE lubrificantes ADD COLUMN tipo TEXT DEFAULT 'óleo'")
                                    cur.execute(
                                        "INSERT INTO lubrificantes (nome, tipo, viscosidade, quantidade_estoque, unidade, observacoes) VALUES (?, ?, ?, ?, ?, ?)",
                                        (nome, tipo, viscosidade, quantidade, unidade, obs)
                                    )
                                    conn.commit()
                                st.success("Lubrificante cadastrado!")
                                st.rerun()

                with sub_tab_componentes:
                    st.subheader("Importar Componentes por Planilha")
                    st.info(
                        "**Colunas obrigatórias:** `nome_componente`, `intervalo_padrao`\n"
                        "**Colunas opcionais:** `lubrificante_nome`, `capacidade_litros`\n\n"
                        "💡 **Dicas:**\n"
                        "• Se o lubrificante não existir, será criado automaticamente\n"
                        "• Componentes duplicados na mesma classe serão ignorados\n"
                        "• Componentes com mesmo nome em classes diferentes serão adicionados normalmente\n"
                        "• O tipo de manutenção (Troca/Remonta) será definido manualmente ao registrar a manutenção\n"
                        "• A capacidade em litros ajuda no controle de estoque"
                    )
                    
                    # Seleção da classe operacional
                    classes_operacionais = sorted([c for c in df_frotas['Classe_Operacional'].unique() if pd.notna(c) and str(c).strip()])
                    classe_selecionada = st.selectbox("Classe Operacional para importação", options=classes_operacionais, 
                                                    help="Selecione a classe onde os componentes serão importados")
                    
                    arquivo_componentes = st.file_uploader("Selecione a planilha de componentes", type=['xlsx'], key="upl_componentes")
                    if arquivo_componentes is not None:
                        try:
                            df_comp_import = pd.read_excel(arquivo_componentes)
                            st.write("**Pré-visualização dos dados:**")
                            st.dataframe(df_comp_import.head())
                            
                            if st.button("Confirmar e Inserir Componentes", type="primary"):
                                with st.spinner("Importando componentes..."):
                                    # Garantir que as tabelas existem
                                    ensure_lubrificantes_schema()
                                    
                                    # Importar componentes
                                    inseridos, duplicados, lubrificantes_criados, msg = importar_componentes_de_planilha(
                                        DB_PATH, arquivo_componentes, classe_selecionada
                                    )
                                
                                if inseridos > 0:
                                    msg_sucesso = f"✅ **{inseridos}** componentes importados com sucesso!"
                                    if duplicados > 0:
                                        msg_sucesso += f"\n⚠️ **{duplicados}** componentes duplicados ignorados"
                                    if lubrificantes_criados > 0:
                                        msg_sucesso += f"\n🛢️ **{lubrificantes_criados}** lubrificantes criados automaticamente"
                                    
                                    st.success(msg_sucesso)
                                    st.info(f"📋 **Detalhes:** {msg}")
                                    rerun_keep_tab("📤 Importar Dados")
                                else:
                                    st.error(f"❌ Erro na importação: {msg}")
                        except Exception as e:
                            st.error(f"Erro ao ler planilha: {e}")
                    
                    # Exemplo de planilha
                    with st.expander("📋 Exemplo de Planilha de Componentes", expanded=False):
                        st.write("**Estrutura recomendada da planilha:**")
                        
                        # Criar DataFrame de exemplo
                        exemplo_data = {
                            'nome_componente': ['Óleo do Motor', 'Filtro de Ar', 'Filtro de Óleo', 'Correia Dentada'],
                            'intervalo_padrao': [5000, 10000, 5000, 80000],
                            'lubrificante_nome': ['Óleo 15W40', 'Sem lubrificante', 'Óleo 15W40', 'Sem lubrificante'],
                            'capacidade_litros': [15.0, 0.0, 0.5, 0.0]
                        }
                        df_exemplo = pd.DataFrame(exemplo_data)
                        
                        st.dataframe(df_exemplo, use_container_width=True)
                        
                        st.write("**Colunas aceitas:**")
                        st.markdown("""
                        - **nome_componente** (obrigatório): Nome do componente
                        - **componente**: Alternativa para nome_componente
                        - **intervalo_padrao** (obrigatório): Intervalo em km/horas
                        - **intervalo**: Alternativa para intervalo_padrao
                        - **lubrificante_nome** (opcional): Nome do lubrificante associado
                        - **lubrificante**: Alternativa para lubrificante_nome
                        - **capacidade_litros** (opcional): Capacidade do componente em litros
                        - **capacidade**: Alternativa para capacidade_litros
                        """)
                        
                        # Botão para download do exemplo
                        csv_exemplo = df_exemplo.to_csv(index=False)
                        st.download_button(
                            label="📥 Baixar Planilha de Exemplo",
                            data=csv_exemplo,
                            file_name="exemplo_componentes.csv",
                            mime="text/csv"
                        )
        if tab_gerir_checklists is not None:
            with tab_gerir_checklists:
                    st.header("✅ Gerir Checklists")
                    
                    # Criar abas para organizar melhor as funcionalidades
                    tab_config, tab_historico = st.tabs(["⚙️ Configuração", "🗑️ Histórico"])
                    
                    if tab_config is not None:
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
                            # Debug: mostrar os valores que serão usados
                            st.write(f"**Debug:** Tentando excluir checklist com:")
                            st.write(f"- Cod_Equip: {checklist_detalhes['Cod_Equip']}")
                            st.write(f"- Título: {checklist_detalhes['titulo_checklist']}")
                            st.write(f"- Data: {checklist_detalhes['data_preenchimento']}")
                            st.write(f"- Turno: {checklist_detalhes['turno']}")
                            
                            # Verificar status do banco antes da exclusão
                            sync_success, sync_msg = force_database_sync()
                            if sync_success:
                                st.info(f"Status do banco: {sync_msg}")
                            
                            success, message = delete_checklist_history(
                                checklist_detalhes['Cod_Equip'],
                                checklist_detalhes['titulo_checklist'],
                                checklist_detalhes['data_preenchimento'],
                                checklist_detalhes['turno']
                            )
                            if success:
                                st.success(message)
                                # Invalidar cache para atualizar contadores
                                force_cache_clear()
                            else:
                                st.error(message)
        
        # Aba de Backup para persistência no Streamlit Cloud
        if tab_backup is not None:
            with tab_backup:
                st.header("💾 Backup e Restauração")
                st.info("Esta seção permite gerenciar backups dos dados para garantir persistência no Streamlit Cloud.")
                
                col_backup, col_restore = st.columns(2)
                
                with col_backup:
                    st.subheader("📤 Criar Backup")
                    st.write("Cria um backup completo dos dados atuais e salva na sessão do Streamlit.")
                    
                    if st.button("💾 Criar Backup", type="primary"):
                        with st.spinner("Criando backup..."):
                            success, message = save_backup_to_session_state()
                            if success:
                                st.success(message)
                                st.info(f"Backup criado em: {st.session_state.get('backup_timestamp', 'N/A')}")
                            else:
                                st.error(message)
                    
                    # Mostrar status do backup atual
                    if 'database_backup' in st.session_state:
                        st.success("✅ Backup disponível na sessão")
                        st.info(f"Último backup: {st.session_state.get('backup_timestamp', 'N/A')}")
                        
                        # Botão para download do backup
                        backup_b64 = st.session_state['database_backup']
                        backup_bytes = base64.b64decode(backup_b64)
                        
                        st.download_button(
                            label="📥 Download do Backup",
                            data=backup_bytes,
                            file_name=f"backup_database_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                            mime="application/json"
                        )
                    else:
                        st.warning("⚠️ Nenhum backup disponível")
                
                with col_restore:
                    st.subheader("📥 Restaurar Backup")
                    st.write("Restaura dados de um backup salvo na sessão.")
                    
                    if 'database_backup' in st.session_state:
                        if st.button("🔄 Restaurar Backup", type="secondary"):
                            with st.spinner("Restaurando backup..."):
                                success, message = restore_backup_from_session_state()
                                if success:
                                    st.success(message)
                                    st.info("Os dados foram restaurados. A aplicação será recarregada.")
                                else:
                                    st.error(message)
                    else:
                        st.info("Crie um backup primeiro para poder restaurar.")
                
                # Seção de informações sobre persistência
                st.markdown("---")
                st.subheader("ℹ️ Sobre Persistência no Streamlit Cloud")
                
                st.info("""
                **Por que os dados voltam após reiniciar?**
                
                O Streamlit Cloud recria o ambiente a cada deploy ou reinicialização, 
                perdendo todos os dados do banco SQLite. Para resolver isso:
                
                1. **Crie um backup** sempre que fizer alterações importantes
                2. **O backup é salvo na sessão** e persiste durante a navegação
                3. **Após reiniciar**, restaure o backup para recuperar os dados
                
                **Dica:** Faça backup antes de sair da aplicação!
                """)
                
                # Backup automático após operações importantes
                if st.button("🔄 Backup Automático", type="secondary"):
                    with st.spinner("Verificando e criando backup automático..."):
                        # Verificar se há dados no banco
                        conn = sqlite3.connect(DB_PATH, check_same_thread=False)
                        cursor = conn.cursor()
                        cursor.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table'")
                        num_tables = cursor.fetchone()[0]
                        conn.close()
                        
                        if num_tables > 0:
                            success, message = save_backup_to_session_state()
                            if success:
                                st.success(f"Backup automático criado: {message}")
                            else:
                                st.error(f"Erro no backup automático: {message}")
                        else:
                            st.warning("Nenhuma tabela encontrada no banco de dados.")
        
        # Aba de Saúde dos Dados
        if tab_saude is not None:
            with tab_saude:
                st.header("⚕️ Saúde dos Dados")
                st.info("Esta seção permite verificar a integridade e qualidade dos dados da aplicação.")
                
                # Verificação de integridade do banco
                st.subheader("🔍 Verificação de Integridade do Banco")
                
                if st.button("🔍 Verificar Integridade", type="primary"):
                    with st.spinner("Verificando integridade dos dados..."):
                        try:
                            conn = sqlite3.connect(DB_PATH, check_same_thread=False)
                            cursor = conn.cursor()
                            
                            # Verificar tabelas existentes
                            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
                            tabelas = [row[0] for row in cursor.fetchall()]
                            
                            # Verificar integridade de cada tabela
                            resultados_integridade = []
                            
                            for tabela in tabelas:
                                try:
                                    # Verificar se a tabela pode ser lida
                                    cursor.execute(f"SELECT COUNT(*) FROM {tabela}")
                                    count = cursor.fetchone()[0]
                                    
                                    # Verificar estrutura da tabela
                                    cursor.execute(f"PRAGMA table_info({tabela})")
                                    colunas = cursor.fetchall()
                                    
                                    resultados_integridade.append({
                                        'tabela': tabela,
                                        'registros': count,
                                        'colunas': len(colunas),
                                        'status': '✅ OK'
                                    })
                                except Exception as e:
                                    resultados_integridade.append({
                                        'tabela': tabela,
                                        'registros': 0,
                                        'colunas': 0,
                                        'status': f'❌ Erro: {str(e)}'
                                    })
                            
                            conn.close()
                            
                            # Exibir resultados
                            st.success("✅ Verificação concluída!")
                            
                            # Criar DataFrame com resultados
                            df_integridade = pd.DataFrame(resultados_integridade)
                            st.dataframe(df_integridade, use_container_width=True)
                            
                            # Resumo
                            total_tabelas = len(resultados_integridade)
                            tabelas_ok = len([r for r in resultados_integridade if '✅' in r['status']])
                            tabelas_erro = total_tabelas - tabelas_ok
                            
                            col1, col2, col3 = st.columns(3)
                            with col1:
                                st.metric("Total de Tabelas", total_tabelas)
                            with col2:
                                st.metric("Tabelas OK", tabelas_ok)
                            with col3:
                                st.metric("Tabelas com Erro", tabelas_erro)
                            
                        except Exception as e:
                            st.error(f"Erro ao verificar integridade: {e}")
                
                # Verificação de dados específicos
                st.markdown("---")
                st.subheader("📊 Verificação de Dados Específicos")
                
                col_verif1, col_verif2 = st.columns(2)
                
                with col_verif1:
                    st.write("**Verificação de Frotas:**")
                    if not df_frotas.empty:
                        frotas_ativas = df_frotas[df_frotas['ATIVO'] == 'ATIVO'].shape[0]
                        frotas_inativas = df_frotas[df_frotas['ATIVO'] != 'ATIVO'].shape[0]
                        
                        st.metric("Frotas Ativas", frotas_ativas)
                        st.metric("Frotas Inativas", frotas_inativas)
                        
                        if frotas_inativas > 0:
                            st.warning(f"⚠️ {frotas_inativas} frotas inativas encontradas")
                    else:
                        st.error("❌ Nenhuma frota cadastrada")
                
                with col_verif2:
                    st.write("**Verificação de Abastecimentos:**")
                    if not df.empty:
                        total_abastecimentos = df.shape[0]
                        abastecimentos_sem_data = df[df['Data'].isna()].shape[0]
                        
                        st.metric("Total Abastecimentos", total_abastecimentos)
                        st.metric("Sem Data", abastecimentos_sem_data)
                        
                        if abastecimentos_sem_data > 0:
                            st.warning(f"⚠️ {abastecimentos_sem_data} abastecimentos sem data")
                    else:
                        st.error("❌ Nenhum abastecimento registrado")
                
                # Verificação de consistência
                st.markdown("---")
                st.subheader("🔗 Verificação de Consistência")
                
                if st.button("🔗 Verificar Consistência", type="secondary"):
                    with st.spinner("Verificando consistência dos dados..."):
                        inconsistencias = []
                        
                        # Verificar se há abastecimentos para frotas inexistentes
                        if not df.empty and not df_frotas.empty:
                            codigos_abastecimento = set(df['Cod_Equip'].unique())
                            codigos_frota = set(df_frotas['Cod_Equip'].unique())
                            
                            codigos_orfas = codigos_abastecimento - codigos_frota
                            if codigos_orfas:
                                inconsistencias.append(f"❌ {len(codigos_orfas)} abastecimentos para frotas inexistentes")
                        
                        # Verificar se há componentes para classes inexistentes
                        if 'df_comp_regras' in locals() and not df_comp_regras.empty and not df_frotas.empty:
                            classes_componentes = set(df_comp_regras['classe_operacional'].unique())
                            classes_frota = set(df_frotas['Classe_Operacional'].unique())
                            
                            classes_orfas = classes_componentes - classes_frota
                            if classes_orfas:
                                inconsistencias.append(f"❌ {len(classes_orfas)} componentes para classes inexistentes")
                        
                        if inconsistencias:
                            st.error("⚠️ Inconsistências encontradas:")
                            for inc in inconsistencias:
                                st.write(inc)
                        else:
                            st.success("✅ Nenhuma inconsistência encontrada!")
        
        # Aba de Gerir Utilizadores
        if tab_gerir_users is not None:
            with tab_gerir_users:
                st.header("👤 Gerir Utilizadores")
                st.info("Esta seção permite gerenciar usuários e suas permissões na aplicação.")
                
                # Verificar se existe sistema de usuários
                if 'users' not in st.session_state:
                    st.session_state['users'] = {}
                
                # Lista de usuários existentes
                st.subheader("📋 Usuários Cadastrados")
                
                if st.session_state['users']:
                    df_users = pd.DataFrame(list(st.session_state['users'].items()), 
                                          columns=['Username', 'Role'])
                    st.dataframe(df_users, use_container_width=True)
                else:
                    st.info("Nenhum usuário cadastrado além do administrador padrão.")
                
                # Adicionar novo usuário
                st.markdown("---")
                st.subheader("➕ Adicionar Novo Usuário")
                
                with st.form("form_add_user", clear_on_submit=True):
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        novo_username = st.text_input("Nome de usuário", key="new_user_username")
                        nova_senha = st.text_input("Senha", type="password", key="new_user_password")
                    
                    with col2:
                        novo_role = st.selectbox("Tipo de usuário", ["user", "admin"], key="new_user_role")
                        novo_email = st.text_input("Email (opcional)", key="new_user_email")
                    
                    submitted_user = st.form_submit_button("➕ Adicionar Usuário", type="primary")
                    if submitted_user:
                        if novo_username and nova_senha:
                            if novo_username not in st.session_state['users']:
                                # Em uma aplicação real, a senha seria criptografada
                                st.session_state['users'][novo_username] = {
                                    'role': novo_role,
                                    'password': nova_senha,  # Em produção, usar hash
                                    'email': novo_email,
                                    'created_at': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                }
                                st.success(f"✅ Usuário '{novo_username}' criado com sucesso!")
                                st.rerun()
                            else:
                                st.error("❌ Nome de usuário já existe!")
                        else:
                            st.warning("⚠️ Preencha todos os campos obrigatórios!")
                
                # Gerenciar usuários existentes
                if st.session_state['users']:
                    st.markdown("---")
                    st.subheader("✏️ Gerenciar Usuários Existentes")
                    
                    usuarios_lista = list(st.session_state['users'].keys())
                    usuario_selecionado = st.selectbox("Selecionar usuário para gerenciar:", usuarios_lista)
                    
                    if usuario_selecionado:
                        user_data = st.session_state['users'][usuario_selecionado]
                        
                        col_info, col_actions = st.columns([2, 1])
                        
                        with col_info:
                            st.write(f"**Usuário:** {usuario_selecionado}")
                            st.write(f"**Tipo:** {user_data['role']}")
                            st.write(f"**Email:** {user_data.get('email', 'Não informado')}")
                            st.write(f"**Criado em:** {user_data.get('created_at', 'N/A')}")
                        
                        with col_actions:
                            if st.button("🗑️ Excluir Usuário", type="secondary"):
                                if usuario_selecionado != 'admin':  # Proteger admin
                                    del st.session_state['users'][usuario_selecionado]
                                    st.success(f"✅ Usuário '{usuario_selecionado}' excluído!")
                                    st.rerun()
                                else:
                                    st.error("❌ Não é possível excluir o usuário administrador!")
                        
                        # Alterar senha
                        st.markdown("---")
                        st.subheader("🔐 Alterar Senha")
                        
                        with st.form("form_change_password", clear_on_submit=True):
                            nova_senha_alt = st.text_input("Nova senha", type="password", key="change_password")
                            confirmar_senha = st.text_input("Confirmar nova senha", type="password", key="confirm_password")
                            
                            if st.form_submit_button("🔐 Alterar Senha", type="primary"):
                                if nova_senha_alt == confirmar_senha:
                                    if nova_senha_alt:
                                        st.session_state['users'][usuario_selecionado]['password'] = nova_senha_alt
                                        st.success("✅ Senha alterada com sucesso!")
                                        st.rerun()
                                    else:
                                        st.warning("⚠️ A nova senha não pode estar vazia!")
                                else:
                                    st.error("❌ As senhas não coincidem!")
                
                # Informações sobre o sistema de usuários
                st.markdown("---")
                st.subheader("ℹ️ Sobre o Sistema de Usuários")
                
                st.info("""
                **Como funciona:**
                
                - **Admin:** Acesso total a todas as funcionalidades
                - **User:** Acesso limitado (apenas página inicial)
                
                **Segurança:**
                
                ⚠️ **ATENÇÃO:** Este é um sistema básico de usuários para demonstração.
                Em produção, implemente:
                
                1. **Criptografia de senhas** (bcrypt, hashlib)
                2. **Autenticação segura** (JWT, sessions)
                3. **Validação de entrada** robusta
                4. **Logs de auditoria** para ações sensíveis
                5. **Rate limiting** para tentativas de login
                
                **Dica:** As senhas são armazenadas em texto plano na sessão atual!
                """)
                        
if __name__ == "__main__":
    main()
