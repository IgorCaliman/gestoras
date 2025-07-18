# app.py (versão com tabelas centralizadas)
import streamlit as st
import pandas as pd
import plotly.express as px
import os
import locale

# --- CONFIGURA A FORMATAÇÃO PARA O PADRÃO BRASILEIRO ---
try:
    locale.setlocale(locale.LC_ALL, 'pt_BR.UTF-8')
except locale.Error:
    st.error("Locale 'pt_BR.UTF-8' não encontrado. A formatação de moeda pode não funcionar como esperado.")
    locale.setlocale(locale.LC_ALL, '')  # Usa o locale padrão do sistema como fallback

# --- Configurações da Página ---
st.set_page_config(layout="wide", page_title="Análise Consolidada de Carteiras")
st.title('📊 Dashboard: Análise de Gestoras e Ativos')

# --- Constantes e Caminhos ---
CAMINHO_DA_PASTA = "."
NOME_ARQUIVO_LISTA_FUNDOS = "lista_completa_fundos_para_analise.xlsx"
ARQUIVO_MARKET_CAP = "market_caps_historicos.parquet"
MESES_PARA_ANALISE = ['202410', '202411', '202412']
PALETA_DE_CORES = ['#B0B8D1', '#5A76A8', '#001D6E']


# (O restante das funções de carregamento de dados permanece o mesmo)
@st.cache_data
def carregar_mapeamento_gestora_fundo(caminho_arquivo_excel):
    if not os.path.exists(caminho_arquivo_excel):
        st.error(f"ERRO: O arquivo de mapeamento '{caminho_arquivo_excel}' não foi encontrado.")
        return None, None
    try:
        df_mapa = pd.read_excel(caminho_arquivo_excel)
        df_mapa = df_mapa[['Gestora', 'Fundo']].dropna().drop_duplicates()
        lista_todos_fundos = df_mapa['Fundo'].unique().tolist()
        return df_mapa, lista_todos_fundos
    except Exception as e:
        st.error(f"Ocorreu um erro ao ler o arquivo Excel de mapeamento: {e}")
        return None, None


# Substitua a função antiga por esta em seu app.py

@st.cache_data
def carregar_dados_historicos(caminho_da_pasta, meses, _lista_de_fundos):
    if not _lista_de_fundos:
        st.warning("A lista de fundos para análise está vazia.")
        return None

    # A linha "caminho_da_pasta = 'dados_filtrados'" foi REMOVIDA.
    # Agora o código usará o caminho principal, como solicitado.
    lista_dfs_completos = []

    for mes in meses:
        try:
            # Os nomes dos arquivos agora terminam com .parquet
            path_blc4 = os.path.join(caminho_da_pasta, f'cda_fi_BLC_4_{mes}.parquet')
            path_blc8 = os.path.join(caminho_da_pasta, f'cda_fi_BLC_8_{mes}.parquet')
            path_pl = os.path.join(caminho_da_pasta, f'cda_fi_PL_{mes}.parquet')

            # Lê os arquivos Parquet, que é muito mais rápido
            df_blc4 = pd.read_parquet(path_blc4)
            df_blc8 = pd.read_parquet(path_blc8)
            df_pl = pd.read_parquet(path_pl)

            if 'DS_ATIVO' in df_blc8.columns:
                df_blc8.rename(columns={'DS_ATIVO': 'CD_ATIVO'}, inplace=True)

        except FileNotFoundError:
            st.error(f"Arquivos .parquet para o mês {mes} não encontrados na pasta principal.")
            st.info("Certifique-se de que os arquivos Parquet filtrados estão na mesma pasta que o app.py.")
            st.stop()
            continue
        
        carteira_unificada = pd.concat([df_blc4, df_blc8], ignore_index=True)
        
        carteira_completa = pd.merge(carteira_unificada, df_pl[['CNPJ_FUNDO_CLASSE', 'VL_PATRIM_LIQ']],
                                     on='CNPJ_FUNDO_CLASSE', how='left')
        lista_dfs_completos.append(carteira_completa)

    return pd.concat(lista_dfs_completos, ignore_index=True) if lista_dfs_completos else None



@st.cache_data
def carregar_dados_marketcap(caminho_arquivo):
    if not os.path.exists(caminho_arquivo):
        st.error(f"Arquivo de Market Cap '{caminho_arquivo}' não encontrado.")
        st.stop()
    return pd.read_parquet(caminho_arquivo)


# --- Lógica Principal do App ---
mapa_gestora_fundo, todos_os_fundos = carregar_mapeamento_gestora_fundo(
    os.path.join(CAMINHO_DA_PASTA, NOME_ARQUIVO_LISTA_FUNDOS))
df_market_caps = carregar_dados_marketcap(os.path.join(CAMINHO_DA_PASTA, ARQUIVO_MARKET_CAP))
dados_brutos = carregar_dados_historicos(CAMINHO_DA_PASTA, MESES_PARA_ANALISE,
                                         tuple(todos_os_fundos) if todos_os_fundos else ())

if dados_brutos is None or dados_brutos.empty or mapa_gestora_fundo is None:
    st.error("Não foi possível carregar os dados necessários. Verifique os arquivos na pasta e os nomes dos fundos.")
    st.stop()

# --- Processamento e Consolidação dos Dados ---
dados_completos = pd.merge(dados_brutos, mapa_gestora_fundo, left_on='DENOM_SOCIAL', right_on='Fundo', how='inner')
for col in ['VL_MERC_POS_FINAL', 'VL_PATRIM_LIQ']:
    if col in dados_completos.columns:
        dados_completos[col] = pd.to_numeric(dados_completos[col], errors='coerce')

tipos_aplic_interesse_acoes = ['Ações', 'Certificado ou recibo de depósito de valores mobiliários',
                               'Ações e outros TVM cedidos em empréstimo']
tipos_ativo_acao = ['Ação ordinária', 'Ação preferencial', 'Certificado de depósito de ações', 'Recibo de subscrição',
                    'UNIT']
dados_acoes = dados_completos[
    (dados_completos['TP_APLIC'].isin(tipos_aplic_interesse_acoes)) &
    (dados_completos['TP_ATIVO'].isin(tipos_ativo_acao))
    ].copy()
dados_acoes.dropna(subset=['CD_ATIVO'], inplace=True)

posicao_consolidada = dados_acoes.groupby(['DT_COMPTC', 'Gestora', 'CD_ATIVO'], as_index=False).agg(
    Valor_Consolidado_R=('VL_MERC_POS_FINAL', 'sum')
)
posicao_consolidada['MesAno'] = pd.to_datetime(posicao_consolidada['DT_COMPTC']).dt.strftime('%Y%m')

df_final = pd.merge(
    posicao_consolidada, df_market_caps,
    left_on=['CD_ATIVO', 'MesAno'], right_on=['Ticker', 'MesAno'], how='left'
)
df_final.rename(columns={"CD_ATIVO": "Ativo", "MarketCap": "Market_Cap_Cia_R"}, inplace=True)

df_final['Perc_Cia'] = pd.NA
mascara_market_cap_valido = df_final['Market_Cap_Cia_R'].notna() & (df_final['Market_Cap_Cia_R'] > 0)
df_final.loc[mascara_market_cap_valido, 'Perc_Cia'] = \
    (df_final.loc[mascara_market_cap_valido, 'Valor_Consolidado_R'] / df_final.loc[
        mascara_market_cap_valido, 'Market_Cap_Cia_R']) * 100

# --- Interface com Abas ---
tab1, tab2 = st.tabs(["Análise por Gestora", "Análise por Ativo"])

# --- ABA 1: ANÁLISE POR GESTORA ---
with tab1:
    st.header("Análise por Gestora", divider='blue')
    st.sidebar.header("Filtro por Gestora")
    lista_gestoras = sorted(df_final['Gestora'].unique())
    if lista_gestoras:
        gestora_selecionada = st.sidebar.selectbox('Selecione a Gestora:', lista_gestoras, key='filtro_gestora')

        dados_gestora = df_final[df_final['Gestora'] == gestora_selecionada].copy()
        datas_disponiveis = sorted(dados_gestora['DT_COMPTC'].unique(), reverse=True)
        mes_selecionado = datas_disponiveis[0] if datas_disponiveis else None

        if mes_selecionado:
            # --- SEÇÃO 1: VISÃO DO MÊS MAIS RECENTE ---
            st.subheader(f"Visão Consolidada - {pd.to_datetime(mes_selecionado).strftime('%B de %Y')}",
                         divider='rainbow')
            dados_gestora_mes = dados_gestora[dados_gestora['DT_COMPTC'] == mes_selecionado].copy()

            pl_gestora_acoes = dados_gestora_mes['Valor_Consolidado_R'].sum()
            dados_gestora_mes['Perc_PL'] = (dados_gestora_mes[
                                                'Valor_Consolidado_R'] / pl_gestora_acoes) * 100 if pl_gestora_acoes > 0 else 0

            soma_perc_pl = dados_gestora_mes['Perc_PL'].sum()
            col1, col2, col3 = st.columns(3)
            col1.metric("PL em Ações (Consolidado)", f"R$ {pl_gestora_acoes:,.2f}")
            col2.metric("Nº de Ativos na Carteira", f"{len(dados_gestora_mes)}")
            col3.metric("Soma % do PL (Verificação)", f"{soma_perc_pl:.2f}%")

            st.subheader("Exposição Total em Ações (Consolidado)")
            tabela_para_exibir = dados_gestora_mes.sort_values(by='Perc_PL', ascending=False)

            df_display = tabela_para_exibir.copy()
            df_display['Valor (R$)'] = df_display['Valor_Consolidado_R'].apply(
                lambda x: locale.currency(x, symbol=True, grouping=True))
            df_display['Market Cap (R$)'] = df_display['Market_Cap_Cia_R'].apply(
                lambda x: 'R$ ' + locale.format_string('%.0f', x, grouping=True) if pd.notna(x) else 'N/A')
            df_display['% do PL'] = df_display['Perc_PL'].apply(lambda x: f'{x:.2f}%')
            df_display['% da Cia'] = df_display['Perc_Cia'].apply(lambda x: f'{x:.2f}%' if pd.notna(x) else 'N/A')

            # --- APLICA CENTRALIZAÇÃO ---
            st.dataframe(
                df_display[['Ativo', 'Valor (R$)', '% do PL', 'Market Cap (R$)', '% da Cia']]
                .style.set_properties(**{'text-align': 'center'}),
                use_container_width=True,
                hide_index=True
            )

            # (O resto da Aba 1, com os gráficos, permanece igual)
            st.markdown("---")
            st.subheader("Análise Visual da Carteira Consolidada")
            col_bar, col_line = st.columns(2)

            with col_bar:
                fig_bar = px.bar(tabela_para_exibir.head(99), x='Perc_PL', y='Ativo', orientation='h',
                                 title='Posições por % do PL', text='Perc_PL')
                fig_bar.update_layout(yaxis={'categoryorder': 'total ascending'}, height=600, xaxis_title="% do PL",
                                      yaxis_title="Ativo")
                fig_bar.update_traces(texttemplate='%{text:.2f}%', textposition='outside')
                st.plotly_chart(fig_bar, use_container_width=True)

            with col_line:
                df_sorted = tabela_para_exibir.sort_values(by='Perc_PL', ascending=False).reset_index()
                df_sorted['CUM_PERC_PL'] = df_sorted['Perc_PL'].cumsum()
                df_sorted['POSICAO_RANK'] = df_sorted.index + 1
                fig_line = px.line(df_sorted, x='POSICAO_RANK', y='CUM_PERC_PL',
                                   title='Curva de Concentração da Carteira', markers=True, hover_name='Ativo')
                fig_line.update_layout(xaxis_title="Ranking das Posições", yaxis_title="% Acumulado do PL",
                                       yaxis_ticksuffix="%")

                if len(df_sorted) >= 5:
                    y_top5 = df_sorted.loc[4, 'CUM_PERC_PL']
                    fig_line.add_annotation(x=5, y=y_top5, text=f"<b>Top 5:</b><br>{y_top5:.1f}%", showarrow=True,
                                            arrowhead=2, ax=-40, ay=-40)
                if len(df_sorted) >= 10:
                    y_top10 = df_sorted.loc[9, 'CUM_PERC_PL']
                    fig_line.add_annotation(x=10, y=y_top10, text=f"<b>Top 10:</b><br>{y_top10:.1f}%", showarrow=True,
                                            arrowhead=2, ax=40, ay=-40)
                st.plotly_chart(fig_line, use_container_width=True)

            st.markdown("---")
            st.subheader(f"Evolução Mensal da Carteira: {gestora_selecionada}", divider='rainbow')

            pl_historico_acoes = dados_gestora.groupby('DT_COMPTC')['Valor_Consolidado_R'].sum().reset_index(
                name='PL_ACOES_MES')
            dados_gestora_evolucao = pd.merge(dados_gestora, pl_historico_acoes, on='DT_COMPTC')
            dados_gestora_evolucao['Perc_PL'] = (dados_gestora_evolucao['Valor_Consolidado_R'] / dados_gestora_evolucao[
                'PL_ACOES_MES']) * 100

            st.markdown("##### Posições em Ações (% do PL)")
            mapa_relevancia_pl = dados_gestora_evolucao.groupby('Ativo')['Perc_PL'].max()
            ordem_ativos_pl = mapa_relevancia_pl.sort_values(ascending=False).index.tolist()

            tabela_pivot_pl = dados_gestora_evolucao.pivot_table(index='Ativo', columns='DT_COMPTC',
                                                                 values='Perc_PL').fillna(0)
            tabela_pivot_pl = tabela_pivot_pl.reindex(ordem_ativos_pl)
            df_plot_pl = tabela_pivot_pl.reset_index().melt(id_vars='Ativo', var_name='Data', value_name='% do PL')
            df_plot_pl['Mês'] = pd.to_datetime(df_plot_pl['Data']).dt.strftime('%b/%y')
            df_plot_pl['Texto_da_Barra'] = df_plot_pl['% do PL'].apply(lambda x: f'{x:.2f}%')

            fig_evol_pl = px.bar(df_plot_pl, x='% do PL', y='Ativo', color='Mês', barmode='group', orientation='h',
                                 title='Comparativo Mensal de Posições (% do PL)', text='Texto_da_Barra',
                                 color_discrete_sequence=PALETA_DE_CORES)
            fig_evol_pl.update_layout(height=max(400, len(ordem_ativos_pl) * 100),
                                      yaxis={'categoryorder': 'array',
                                             'categoryarray': list(reversed(ordem_ativos_pl))},
                                      yaxis_title="Ativo", xaxis_title="% do PL Consolidado")
            fig_evol_pl.update_traces(textposition='outside')
            st.plotly_chart(fig_evol_pl, use_container_width=True)

            st.markdown("---")
            st.markdown("##### Participação nas Companhias (% da Cia)")
            dados_evolucao_cia = dados_gestora_evolucao.dropna(subset=['Perc_Cia'])
            if not dados_evolucao_cia.empty:
                mapa_relevancia_cia = dados_evolucao_cia.groupby('Ativo')['Perc_Cia'].max()
                ordem_ativos_cia = mapa_relevancia_cia.sort_values(ascending=False).index.tolist()

                tabela_pivot_cia = dados_evolucao_cia.pivot_table(index='Ativo', columns='DT_COMPTC',
                                                                  values='Perc_Cia').fillna(0)
                tabela_pivot_cia = tabela_pivot_cia.reindex(ordem_ativos_cia)
                df_plot_cia = tabela_pivot_cia.reset_index().melt(id_vars='Ativo', var_name='Data',
                                                                  value_name='% da Cia')
                df_plot_cia['Mês'] = pd.to_datetime(df_plot_cia['Data']).dt.strftime('%b/%y')
                df_plot_cia['Texto_da_Barra'] = df_plot_cia['% da Cia'].apply(lambda x: f'{x:.2f}%')

                fig_evol_cia = px.bar(df_plot_cia, x='% da Cia', y='Ativo', color='Mês', barmode='group',
                                      orientation='h',
                                      title='Comparativo Mensal de Participação (% da Companhia)',
                                      text='Texto_da_Barra',
                                      color_discrete_sequence=PALETA_DE_CORES)
                fig_evol_cia.update_layout(height=max(400, len(ordem_ativos_cia) * 100),
                                           yaxis={'categoryorder': 'array',
                                                  'categoryarray': list(reversed(ordem_ativos_cia))},
                                           yaxis_title="Ativo", xaxis_title="% da Companhia")
                fig_evol_cia.update_traces(textposition='outside')
                st.plotly_chart(fig_evol_cia, use_container_width=True)

        else:
            st.warning(f"Nenhum dado de ações encontrado para a gestora {gestora_selecionada}.")
    else:
        st.warning("Nenhuma gestora encontrada nos dados processados.")

# --- ABA 2: ANÁLISE POR ATIVO ---
with tab2:
    st.header("Análise por Ativo", divider='blue')
    st.sidebar.header("Filtro por Ativo")
    st.sidebar.divider()
    lista_ativos = sorted(df_final['Ativo'].unique())
    if lista_ativos:
        datas_disponiveis_geral = sorted(df_final['DT_COMPTC'].unique(), reverse=True)
        if datas_disponiveis_geral:
            default_index = lista_ativos.index('COGN3') if 'COGN3' in lista_ativos else 0
            ativo_selecionado = st.sidebar.selectbox("Selecione o Ativo:", options=lista_ativos, index=default_index,
                                                     key='filtro_ativo')

            mes_recente_geral = datas_disponiveis_geral[0]
            df_filtrado_ativo = df_final[
                (df_final['Ativo'] == ativo_selecionado) & (df_final['DT_COMPTC'] == mes_recente_geral)].copy()

            st.subheader(f"Investidores para o Ativo: {ativo_selecionado}", divider='rainbow')
            if not df_filtrado_ativo.empty:
                st.write("**Posições das Gestoras da Lista**")

                df_display_ativo = df_filtrado_ativo.copy()
                df_display_ativo['Posição (R$)'] = df_display_ativo['Valor_Consolidado_R'].apply(
                    lambda x: locale.currency(x, symbol=True, grouping=True))
                df_display_ativo['% da Cia'] = df_display_ativo['Perc_Cia'].apply(
                    lambda x: f'{x:.2f}%' if pd.notna(x) else 'N/A')

                # --- APLICA CENTRALIZAÇÃO ---
                st.dataframe(
                    df_display_ativo[['Gestora', 'Posição (R$)', '% da Cia']].sort_values(by="% da Cia",
                                                                                          ascending=False)
                    .style.set_properties(**{'text-align': 'center'}),
                    use_container_width=True,
                    hide_index=True
                )

                st.markdown("---")
                total_perc_gestoras = df_filtrado_ativo['Perc_Cia'].sum()
                perc_outros = 100 - total_perc_gestoras

                df_para_donut = df_filtrado_ativo[['Gestora', 'Perc_Cia']].copy()
                if perc_outros > 0:
                    outros_row = pd.DataFrame([{'Gestora': 'Outros Acionistas', 'Perc_Cia': perc_outros}])
                    df_para_donut = pd.concat([df_para_donut, outros_row], ignore_index=True)

                st.write(f"**Contexto de Propriedade de {ativo_selecionado}**")
                fig_rosca = px.pie(
                    df_para_donut, names='Gestora', values='Perc_Cia',
                    title=f"Distribuição da Participação em {ativo_selecionado}", hole=0.4
                )
                fig_rosca.update_traces(textinfo='percent+label', textposition='inside')
                st.plotly_chart(fig_rosca, use_container_width=True)

            else:
                st.warning("Nenhum dado encontrado para o ativo selecionado.")
    else:
        st.warning("Nenhum ativo encontrado nos dados processados.")
