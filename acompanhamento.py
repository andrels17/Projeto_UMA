import streamlit as st
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime
import os
import plotly.express as px

# ---------------- Configurações Globais ----------------
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(SCRIPT_DIR, "frotas_data.db")

ALERTAS_MANUTENCAO = {
    'HORAS': { 'default': 20 },
    'QUILÔMETROS': { 'default': 500 }
}

# ---------------- Funções Utilitárias ----------------
def formatar_brasileiro(valor: float, prefixo='') -> str:
    """Formata um número com casas decimais para o padrão brasileiro."""
    if pd.isna(valor) or not np.isfinite(valor):
        return "–"
    return f"{prefixo}{valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

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

# APAGUE A SUA FUNÇÃO "load_data_from_db" INTEIRA E SUBSTITUA-A POR ESTE BLOCO FINAL

# APAGUE A SUA FUNÇÃO "load_data_from_db" INTEIRA E SUBSTITUA-A POR ESTE BLOCO FINAL

@st.cache_data(show_spinner="Carregando dados...")
def load_data_from_db(db_path: str) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Carrega todos os dados necessários do DB."""
    if not os.path.exists(db_path):
        st.error(f"Arquivo de banco de dados '{db_path}' não encontrado.")
        st.stop()
    try:
        with sqlite3.connect(db_path, check_same_thread=False) as conn:
            df_abast = pd.read_sql_query("SELECT rowid, * FROM abastecimentos", conn)
            df_frotas = pd.read_sql_query("SELECT * FROM frotas", conn)
            df_manutencoes = pd.read_sql_query("SELECT rowid, * FROM manutencoes", conn)
    except Exception as e:
        if "no such table: manutencoes" in str(e):
            st.error("A tabela 'manutencoes' não foi encontrada. Por favor, execute o comando SQL para criá-la.")
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
            if df[col].dtype == 'object':
                series = df[col].astype(str).str.replace(',', '.', regex=False).str.replace('-', '', regex=False)
                df[col] = pd.to_numeric(series, errors='coerce')
            else:
                df[col] = pd.to_numeric(df[col], errors='coerce')

    df_frotas["label"] = df_frotas["Cod_Equip"].astype(str) + " - " + df_frotas.get("DESCRICAO_EQUIPAMENTO", "").fillna("") + " (" + df_frotas.get("PLACA", "").fillna("Sem Placa") + ")"

    # --- INÍCIO DA CORREÇÃO DEFINITIVA (com base na sua análise) ---
    
    # 1. Cria um mapa da Classe Operacional mais completa a partir da tabela unificada 'df'
    #    Isto garante que usamos a informação tanto da tabela de frotas como da de abastecimentos
    classe_map = df.dropna(subset=['Classe_Operacional']).groupby('Cod_Equip')['Classe_Operacional'].first()
    
    # 2. Atualiza a tabela 'df_frotas' com esta informação mais completa
    df_frotas['Classe_Operacional'] = df_frotas['Cod_Equip'].map(classe_map)

    def determinar_tipo_controle(row):
        # Agora esta função usa a Classe Operacional corrigida
        texto_para_verificar = (
            str(row.get('DESCRICAO_EQUIPAMENTO', '')) + ' ' + 
            str(row.get('Classe_Operacional', ''))
        ).upper()
        
        km_keywords = ['CAMINH', 'VEICULO', 'PICKUP', 'CAVALO MECANICO']
        
        if any(p in texto_para_verificar for p in km_keywords):
            return 'QUILÔMETROS'
        else:
            return 'HORAS'

    # 3. Aplica a função de determinação do tipo, agora com os dados corretos
    df_frotas['Tipo_Controle'] = df_frotas.apply(determinar_tipo_controle, axis=1)
    # --- FIM DA CORREÇÃO DEFINITIVA ---
    
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

# NOVA FUNÇÃO para excluir um abastecimento
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
    """Insere um novo registro de manutenção no banco de dados."""
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
        # Nomes das colunas devem corresponder exatamente ao seu .db
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
    
# COLE ESTE BLOCO DE CÓDIGO NO LOCAL INDICADO

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

@st.cache_data
def filtrar_dados(df: pd.DataFrame, opts: dict) -> pd.DataFrame:
    # Assegura que a coluna 'Mes' é tratada como string para o filtro funcionar
    df_copy = df.copy()
    if 'Mes' in df_copy.columns:
        df_copy['Mes'] = df_copy['Mes'].astype(str)
    
    if "Classe_Operacional" not in df_copy.columns:
        return pd.DataFrame()
        
    mask = (df_copy["Safra"].isin(opts["safras"])) & \
           (df_copy["Ano"].isin(opts["anos"])) & \
           (df_copy["Mes"].isin(opts["meses"])) & \
           (df_copy["Classe_Operacional"].isin(opts["classes_op"]))
    return df_copy.loc[mask]

# SUBSTITUA A SUA FUNÇÃO "build_maintenance_plan" POR ESTE BLOCO
@st.cache_data(show_spinner="Calculando plano de manutenção...")
def build_maintenance_plan(_df_frotas: pd.DataFrame, _df_abastecimentos: pd.DataFrame, _df_manutencoes: pd.DataFrame, intervalos_por_classe: dict) -> pd.DataFrame:
    latest_readings = _df_abastecimentos.sort_values('Data').groupby('Cod_Equip')['Hod_Hor_Atual'].last()
    plan_data = []

    for _, frota_row in _df_frotas.iterrows():
        cod_equip = frota_row['Cod_Equip']
        tipo_controle = frota_row['Tipo_Controle']
        classe_op = frota_row.get('Classe Operacional')
        hod_hor_atual = latest_readings.get(cod_equip)

        if pd.isna(hod_hor_atual) or not classe_op: continue

        # Pega os serviços aplicáveis para ESTA CLASSE ESPECÍFICA
        servicos_aplicaveis = intervalos_por_classe.get(classe_op, {})
        if not servicos_aplicaveis: continue # Pula se não houver configuração para esta classe

        unidade = 'km' if tipo_controle == 'QUILÔMETROS' else 'h'
        alerta_default = ALERTAS_MANUTENCAO.get(tipo_controle, {}).get('default', 500)
        
        record = {'Cod_Equip': cod_equip, 'Equipamento': frota_row.get('DESCRICAO_EQUIPAMENTO', 'N/A'), 'Leitura_Atual': hod_hor_atual, 'Unidade': unidade, 'Qualquer_Alerta': False}
        
        for servico, intervalo in servicos_aplicaveis.items():
            if not intervalo or intervalo <= 0: continue # Ignora intervalos inválidos
            manutencoes_servico = _df_manutencoes[(_df_manutencoes['Cod_Equip'] == cod_equip) & (_df_manutencoes['Tipo_Servico'] == servico)]
            ultimo_servico_hod_hor = 0
            if not manutencoes_servico.empty: ultimo_servico_hod_hor = manutencoes_servico['Hod_Hor_No_Servico'].max()
            
            base_calculo = ultimo_servico_hod_hor if hod_hor_atual >= ultimo_servico_hod_hor else hod_hor_atual
            multiplicador = (hod_hor_atual - base_calculo) // intervalo
            prox_servico = base_calculo + (multiplicador + 1) * intervalo

            restante = prox_servico - hod_hor_atual
            alerta = restante <= alerta_default
            
            if alerta: record['Qualquer_Alerta'] = True
            record[f'Prox_{servico}'] = prox_servico
            record[f'Restante_{servico}'] = restante
            record[f'Alerta_{servico}'] = alerta

        plan_data.append(record)
        
    if not plan_data: return pd.DataFrame()
    return pd.DataFrame(plan_data)

# ---------------- App principal ----------------
def main():
    st.set_page_config(page_title="Dashboard de Frotas", layout="wide")
    st.title("📊 Dashboard de Frotas e Abastecimentos")

    df, df_frotas, df_manutencoes = load_data_from_db(DB_PATH)

# APAGUE O SEU BLOCO DE INICIALIZAÇÃO DE INTERVALOS E SUBSTITUA-O POR ESTE

# Lógica para inicializar e gerir os intervalos por classe na sessão
    if 'intervalos_por_classe' not in st.session_state:
        st.session_state.intervalos_por_classe = {}
    
    # --- INÍCIO DA CORREÇÃO ---
    # Filtra classes nulas (None) ou vazias antes de criar as configurações
    classes_operacionais = [
        classe for classe in df_frotas['Classe Operacional'].unique() 
        if pd.notna(classe) and str(classe).strip() != ''
    ]
    # --- FIM DA CORREÇÃO ---
    
    for classe in classes_operacionais:
        if classe not in st.session_state.intervalos_por_classe:
            # A função iloc[0] garante que pegamos o tipo de controle mesmo que haja múltiplas linhas para a classe
            tipo_controle = df_frotas[df_frotas['Classe Operacional'] == classe]['Tipo_Controle'].iloc[0]
            if tipo_controle == 'HORAS':
                st.session_state.intervalos_por_classe[classe] = {'Lubrificacao': 250, 'Revisao_1': 100, 'Revisao_2': 300, 'Revisao_3': 500}
            else: # QUILÔMETROS
                st.session_state.intervalos_por_classe[classe] = {'Lubrificacao': 5000, 'Revisao_1': 5000, 'Revisao_2': 10000, 'Revisao_3': 20000}

    with st.sidebar:
        st.header("📅 Filtros")
        safra_opts = sorted(list(df["Safra"].dropna().unique())) if "Safra" in df else []
        ano_opts = sorted(list(df["Ano"].dropna().unique())) if "Ano" in df else []
        mes_opts = sorted(list(df["Mes"].dropna().astype(str).unique())) if "Mes" in df else []
        classe_opts = sorted(list(df["Classe_Operacional"].dropna().unique())) if "Classe_Operacional" in df else []

        sel_safras = st.multiselect("Safra", safra_opts, default=safra_opts[-1:] if safra_opts else [])
        sel_anos = st.multiselect("Ano", ano_opts, default=ano_opts[-1:] if ano_opts else [])
        sel_meses = st.multiselect("Mês", mes_opts, default=mes_opts)
        sel_classes = st.multiselect("Classe Operacional", classe_opts, default=classe_opts)
        
        opts = {"safras": sel_safras or safra_opts, "anos": sel_anos or ano_opts, "meses": sel_meses or mes_opts, "classes_op": sel_classes or classe_opts}

    df_f = filtrar_dados(df, opts)

    plan_df = build_maintenance_plan(df_frotas, df, df_manutencoes, st.session_state.intervalos_por_classe)

    tabs = ["📊 Análise Geral", "🛠️ Controle de Manutenção", "🔎 Consulta Individual", "⚙️ Gerir Lançamentos", "⚙️ Configurações"]
    tab_analise, tab_manut, tab_consulta, tab_gerir, tab_config = st.tabs(tabs)

    with tab_analise:
        st.header("Visão Geral de Consumo")

        # Primeiro, verifica se há dados filtrados para evitar erros
        if not df_f.empty:
            # Bloco para exibir as métricas (KPIs)
            if 'Media' in df_f.columns:
                k1, k2 = st.columns(2)
                k1.metric("Litros Consumidos (período)", formatar_brasileiro_int(df_f["Qtde_Litros"].sum()))
                k2.metric("Média Consumo (período)", f"{formatar_brasileiro(df_f['Media'].mean())}")
            else:
                k1 = st.columns(1)[0] # Usa st.columns para manter o layout consistente
                k1.metric("Litros Consumidos (período)", formatar_brasileiro_int(df_f["Qtde_Litros"].sum()))

            # CORREÇÃO: Bloco para exibir os gráficos, agora com a indentação correta
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

            # Filtra dados para garantir que a média seja calculada apenas com valores válidos
            df_media = df_f[(df_f['Media'].notna()) & (df_f['Media'] > 0)].copy()

            # --- INÍCIO DA CORREÇÃO ---
            # Lista das classes a serem excluídas do gráfico
            classes_para_excluir = ['MOTOCICLETA', 'VEICULOS LEVES', 'USINA', 'MINI CARREGADEIRA']

            # Filtra o DataFrame para remover essas classes (comparando em maiúsculas para garantir)
            df_media_filtrado = df_media[~df_media['Classe_Operacional'].str.upper().isin(classes_para_excluir)]
            # --- FIM DA CORREÇÃO ---


            if not df_media_filtrado.empty: # Usa o novo DataFrame filtrado
                # Calcula a média de consumo por classe e ordena
                media_por_classe = df_media_filtrado.groupby('Classe_Operacional')['Media'].mean().sort_values(ascending=True)
                
                df_media_grafico = media_por_classe.reset_index()
                
                # Formata o texto do rótulo para o padrão brasileiro
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
        
        # --- INÍCIO DAS MELHORIAS ---
        
        col_grafico, col_alerta = st.columns([2, 1]) # Divide a área em duas colunas

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

            if pd.notna(media_equip_selecionado) and pd.notna(media_da_classe):
                # 1. Lógica para o Alerta de Eficiência
                with col_alerta:
                    st.write("") # Espaçamento
                    st.write("") # Espaçamento
                    if media_equip_selecionado <= media_da_classe * 1.05: # 5% de tolerância
                        st.success(f"**EFICIENTE!** O consumo está dentro ou abaixo da média da sua classe.")
                    else:
                        st.error(f"**ALERTA!** O consumo está acima da média da sua classe.")
                    
                    st.metric(label=f"Média do Equipamento", value=formatar_brasileiro(media_equip_selecionado))
                    st.metric(label=f"Média da Classe", value=formatar_brasileiro(media_da_classe))

                # 2. Gráfico com tamanho e formatação ajustados
                with col_grafico:
                    df_comp = pd.DataFrame({
                        'Categoria': [dados_eq.get('DESCRICAO_EQUIPAMENTO'), f"Média da Classe ({classe_selecionada})"],
                        'Média Consumo': [media_equip_selecionado, media_da_classe]
                    })
                    fig_comp = px.bar(df_comp, x='Categoria', y='Média Consumo', text='Média Consumo', title="Eficiência de Consumo")
                    
                    # Formata os números para o padrão brasileiro com 2 casas decimais
                    fig_comp.update_traces(texttemplate='%{text:,.2f}'.replace(",", "X").replace(".", ",").replace("X", "."), textposition='outside')
                    
                    # Diminui a altura do gráfico
                    fig_comp.update_layout(height=400)
                    st.plotly_chart(fig_comp, use_container_width=True)
            else:
                col_grafico.info("Não há dados de consumo suficientes para gerar o comparativo.")
        
        # --- FIM DAS MELHORIAS ---
        
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
                
                # --- INÍCIO DA CORREÇÃO 1 ---
                df_alertas_display = df_com_alerta[display_cols].copy()
                df_alertas_display['Leitura_Atual'] = df_alertas_display['Leitura_Atual'].apply(
                    lambda x: formatar_brasileiro_int(x) if pd.notna(x) else ''
                )
                st.dataframe(
                    df_alertas_display,
                    column_config={"Cod_Equip": st.column_config.NumberColumn(format="%d")}
                )
                # --- FIM DA CORREÇÃO 1 ---

            else:
                st.success("✅ Nenhum equipamento com alerta no momento.")

            with st.expander("Ver Plano de Manutenção Completo (Quanto Falta)"):
                cols_to_show = ['Cod_Equip', 'Equipamento', 'Leitura_Atual']
                for col in plan_df.columns:
                    if 'Restante_' in col and plan_df[col].notna().any():
                        cols_to_show.append(col)
                
                # --- INÍCIO DA CORREÇÃO 2 ---
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

        # APAGUE O SEU BLOCO "with st.form(...)" E SUBSTITUA-O POR ESTE BLOCO CORRIGIDO

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
        with tab_gerir:
            st.header("⚙️ Gerir Lançamentos e Frotas")
            
            acao = st.radio(
                "Selecione a ação que deseja realizar:",
                ("Adicionar Lançamento", "Editar Lançamento", "Excluir Lançamento", "Cadastrar Nova Frota"),
                horizontal=True
            )
        
            if acao == "Adicionar Lançamento":
        
                st.subheader("➕ Adicionar Novo Abastecimento")
                with st.form("form_abastecimento", clear_on_submit=True):
                    equip_selecionado_label = st.selectbox("Selecione o Equipamento", options=df_frotas.sort_values("label")["label"])
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
                            classe_op = df_frotas.loc[df_frotas['Cod_Equip'] == cod_equip, 'Classe Operacional'].iloc[0]

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

            elif acao == "Cadastrar Nova Frota":
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

            elif acao == "Excluir Lançamento":
                        st.subheader("🗑️ Excluir um Lançamento")
                        
                        # Criar uma lista de abastecimentos para seleção
                        # Usamos o `df` original que contém todos os dados, incluindo o 'rowid'
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
                                        'data': nova_data.strftime("%Y-%m-%d %H:%M:%S"), # Mantém o formato de data e hora
                                        'qtde_litros': nova_qtde,
                                        'hod_hor_atual': novo_hod,
                                        'safra': nova_safra
                                    }
                                    if editar_abastecimento(DB_PATH, rowid_selecionado, dados_editados):
                                        st.success("Abastecimento atualizado com sucesso!")
                                        st.cache_data.clear()
                                        st.rerun()

# APAGUE O CONTEÚDO DE "if tipo_edicao == 'Manutenção':" E SUBSTITUA-O POR ESTE BLOCO FINAL

# APAGUE O CONTEÚDO DE "if tipo_edicao == 'Manutenção':" E SUBSTITUA-O POR ESTE BLOCO FINAL

                        if tipo_edicao == "Manutenção":
                            st.subheader("Editar Lançamento de Manutenção")
                            
                            # Junta as manutenções com as descrições dos equipamentos para o selectbox
                            df_manut_edit = pd.merge(df_manutencoes, df_frotas[['Cod_Equip', 'DESCRICAO_EQUIPAMENTO']], on='Cod_Equip', how='left')
                            df_manut_edit.sort_values(by="Data", ascending=False, inplace=True)
                            df_manut_edit['Data'] = pd.to_datetime(df_manut_edit['Data'])

                            df_manut_edit['label_edit'] = (
                                            df_manut_edit['Data'].dt.strftime('%d/%m/%Y') + " | Frota: " +
                                            df_manut_edit['Cod_Equip'].astype(str) + " - " +
                                            df_manut_edit['DESCRICAO_EQUIPAMENTO'].fillna('N/A') + " | " +
                                            df_manut_edit['Tipo_Servico'] + " | " +
                                            df_manut_edit['Hod_Hor_No_Servico'].apply(lambda x: formatar_brasileiro_int(x)) + " h/km"
                                        )

                            # --- INÍCIO DA CORREÇÃO DEFINITIVA ---
                            # Método alternativo e mais robusto para criar o dicionário, evitando o erro
                            map_label_to_rowid = dict(zip(df_manut_edit['label_edit'], df_manut_edit['rowid']))
                            # --- FIM DA CORREÇÃO DEFINITIVA ---
                            
                            label_selecionado = st.selectbox("Selecione a manutenção para editar", options=df_manut_edit['label_edit'], key="manut_edit_select")
                            
                            if label_selecionado:
                                rowid_selecionado = map_label_to_rowid.get(label_selecionado)
                                if rowid_selecionado:
                                    dados_atuais = df_manutencoes[df_manutencoes['rowid'] == rowid_selecionado].iloc[0]

                                    with st.form("form_edit_manutencao"):
                                        st.write(f"**Editando:** {label_selecionado}")

                                        lista_labels_frotas = df_frotas.sort_values("label")['label'].tolist()
                                        equip_atual = df_frotas[df_frotas['Cod_Equip'] == dados_atuais['Cod_Equip']]['label'].iloc[0]
                                        index_equip_atual = lista_labels_frotas.index(equip_atual)
                                        
                                        novo_equip_label = st.selectbox("Equipamento", options=lista_labels_frotas, index=index_equip_atual)
                                        
                                        classe_selecionada = df_frotas[df_frotas['label'] == novo_equip_label]['Classe Operacional'].iloc[0]
                                        servicos_disponiveis = list(st.session_state.intervalos_por_classe.get(classe_selecionada, {}).keys())
                                        
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
                                    
        with tab_config:
            st.header("⚙️ Configurar Intervalos de Manutenção por Classe")
            st.info("As alterações feitas aqui são salvas automaticamente para a sua sessão atual.")

            for classe, servicos in st.session_state.intervalos_por_classe.items():
                with st.expander(f"**{classe}**"):
                    novos_servicos = {}
                    for nome_servico, intervalo in servicos.items():
                        novo_intervalo = st.number_input(
                            label=f"{nome_servico} (intervalo)",
                            value=intervalo,
                            min_value=0,
                            step=100,
                            key=f"{classe}_{nome_servico}"
                        )
                        novos_servicos[nome_servico] = novo_intervalo
                    st.session_state.intervalos_por_classe[classe] = novos_servicos
                    
if __name__ == "__main__":
    main()
