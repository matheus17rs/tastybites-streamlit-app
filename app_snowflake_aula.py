"""
APP STREAMLIT + SNOWFLAKE - AULA UNICAMP
Demonstração de Integração com Data Warehouse
Prof. Francisco Fambrini
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import snowflake.connector
from datetime import datetime

# ============================================================================
# CONFIGURAÇÃO DA PÁGINA
# ============================================================================

st.set_page_config(
    page_title="TastyBytes Analytics",
    page_icon="🍕",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ============================================================================
# FUNÇÕES DE CONEXÃO
# ============================================================================

@st.cache_resource
def init_connection():
    """Cria conexão com Snowflake usando secrets.toml"""
    return snowflake.connector.connect(
        user=st.secrets["snowflake"]["user"],
        password=st.secrets["snowflake"]["password"],
        account=st.secrets["snowflake"]["account"],
        warehouse=st.secrets["snowflake"]["warehouse"],
        database=st.secrets["snowflake"]["database"],
        schema=st.secrets["snowflake"]["schema"],
        role=st.secrets["snowflake"]["role"]
    )

@st.cache_data(ttl=600)
def run_query(query):
    """Executa query e retorna DataFrame (cache de 10 minutos)"""
    conn = init_connection()
    cur = conn.cursor()
    try:
        cur.execute(query)
        df = cur.fetch_pandas_all()
        
        # Converter colunas numéricas automaticamente
        for col in df.columns:
            # Tentar converter para numérico
            df[col] = pd.to_numeric(df[col], errors='ignore')
        
        return df
    finally:
        cur.close()

# ============================================================================
# HEADER
# ============================================================================

st.title("🍕 TastyBytes - Analytics Dashboard")
st.markdown("**Integração Streamlit + Snowflake Data Warehouse**")

# ============================================================================
# SIDEBAR
# ============================================================================

with st.sidebar:
    st.header("⚙️ Configurações")
    
    # Verificar conexão
    try:
        conn = init_connection()
        st.success("✅ Conectado ao Snowflake")
        
        with st.expander("ℹ️ Detalhes da Conexão"):
            st.code(f"""
Usuário: {st.secrets["snowflake"]["user"]}
Conta: {st.secrets["snowflake"]["account"]}
Database: {st.secrets["snowflake"]["database"]}
Warehouse: {st.secrets["snowflake"]["warehouse"]}
Região: São Paulo (sa-east-1)
            """)
        
        conectado = True
        
    except Exception as e:
        st.error(f"❌ Erro: {str(e)}")
        st.stop()
    
    st.markdown("---")
    
    # Info
    st.markdown("### 📚 Sobre Este Projeto")
    st.info("""
    **Demonstração Acadêmica**
    
    Este dashboard conecta em tempo real 
    ao Snowflake Data Warehouse e analisa 
    dados do TastyBytes (food truck global).
    
    **Tecnologias:**
    - 🐍 Python
    - 📊 Streamlit
    - ❄️ Snowflake
    - 📈 Plotly
    """)
    
    st.markdown("---")
    st.caption(f"⏱️ Atualizado: {datetime.now().strftime('%H:%M:%S')}")

# ============================================================================
# CONTEÚDO PRINCIPAL
# ============================================================================

if conectado:
    
    # Criar tabs
    tab1, tab2, tab3, tab4 = st.tabs([
        "📊 Dashboard", 
        "🔍 Explorar Dados", 
        "💻 Query SQL",
        "📚 Tutorial"
    ])
    
    # ========================================================================
    # TAB 1: DASHBOARD
    # ========================================================================
    
    with tab1:
        st.header("📊 Análise do Cardápio TastyBytes")
        
        # Carregar dados
        with st.spinner("Carregando dados do Snowflake..."):
            df = run_query("""
                SELECT 
                    MENU_ITEM_NAME,
                    ITEM_CATEGORY,
                    ITEM_SUBCATEGORY,
                    COST_OF_GOODS_USD,
                    SALE_PRICE_USD,
                    (SALE_PRICE_USD - COST_OF_GOODS_USD) AS PROFIT,
                    ROUND(((SALE_PRICE_USD - COST_OF_GOODS_USD) / SALE_PRICE_USD) * 100, 2) AS MARGIN_PERCENT
                FROM MENU
            """)
        
        st.success(f"✅ {len(df)} itens carregados do warehouse")
        
        # KPIs
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Total de Itens", len(df))
        with col2:
            st.metric("Preço Médio", f"${df['SALE_PRICE_USD'].mean():.2f}")
        with col3:
            st.metric("Lucro Médio/Item", f"${df['PROFIT'].mean():.2f}")
        with col4:
            st.metric("Margem Média", f"{df['MARGIN_PERCENT'].mean():.1f}%")
        
        st.markdown("---")
        
        # Gráficos lado a lado
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("📊 Distribuição por Categoria")
            
            cat_count = df['ITEM_CATEGORY'].value_counts().reset_index()
            cat_count.columns = ['Categoria', 'Quantidade']
            
            fig1 = px.pie(cat_count, values='Quantidade', names='Categoria',
                         title="Itens por Categoria",
                         color_discrete_sequence=px.colors.qualitative.Set3)
            st.plotly_chart(fig1, use_container_width=True)
        
        with col2:
            st.subheader("💰 Top 10 - Maior Margem")
            
            top10 = df.nlargest(10, 'MARGIN_PERCENT')[['MENU_ITEM_NAME', 'MARGIN_PERCENT']]
            
            fig2 = px.bar(top10, x='MARGIN_PERCENT', y='MENU_ITEM_NAME',
                         orientation='h', 
                         title="Produtos Mais Lucrativos (%)",
                         color='MARGIN_PERCENT',
                         color_continuous_scale='Greens',
                         labels={'MARGIN_PERCENT': 'Margem (%)', 
                                'MENU_ITEM_NAME': 'Produto'})
            st.plotly_chart(fig2, use_container_width=True)
        
        # Gráfico de dispersão
        st.subheader("🎯 Análise: Custo vs Preço")
        
        fig3 = px.scatter(df, 
                         x='COST_OF_GOODS_USD', 
                         y='SALE_PRICE_USD',
                         color='ITEM_CATEGORY',
                         size='PROFIT',
                         hover_data=['MENU_ITEM_NAME'],
                         title="Relação Custo x Preço de Venda",
                         labels={'COST_OF_GOODS_USD': 'Custo (USD)', 
                                'SALE_PRICE_USD': 'Preço de Venda (USD)'},
                         template='plotly_white')
        
        st.plotly_chart(fig3, use_container_width=True)
        
        # Box plot
        st.subheader("📦 Distribuição de Preços por Categoria")
        
        fig4 = px.box(df, x='ITEM_CATEGORY', y='SALE_PRICE_USD',
                     color='ITEM_CATEGORY',
                     title="Variação de Preços",
                     labels={'SALE_PRICE_USD': 'Preço (USD)', 
                            'ITEM_CATEGORY': 'Categoria'})
        st.plotly_chart(fig4, use_container_width=True)
    
    # ========================================================================
    # TAB 2: EXPLORAR DADOS
    # ========================================================================
    
    with tab2:
        st.header("🔍 Explorar Dados Detalhados")
        
        # Filtros
        st.subheader("Filtros")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            categorias = st.multiselect(
                "Categorias",
                options=df['ITEM_CATEGORY'].unique(),
                default=df['ITEM_CATEGORY'].unique()
            )
        
        with col2:
            subcategorias = st.multiselect(
                "Subcategorias",
                options=df['ITEM_SUBCATEGORY'].unique(),
                default=df['ITEM_SUBCATEGORY'].unique()
            )
        
        with col3:
            preco_range = st.slider(
                "Faixa de Preço (USD)",
                min_value=float(df['SALE_PRICE_USD'].min()),
                max_value=float(df['SALE_PRICE_USD'].max()),
                value=(float(df['SALE_PRICE_USD'].min()), float(df['SALE_PRICE_USD'].max()))
            )
        
        # Filtrar dados
        df_filtrado = df[
            (df['ITEM_CATEGORY'].isin(categorias)) &
            (df['ITEM_SUBCATEGORY'].isin(subcategorias)) &
            (df['SALE_PRICE_USD'] >= preco_range[0]) &
            (df['SALE_PRICE_USD'] <= preco_range[1])
        ]
        
        st.markdown(f"**{len(df_filtrado)}** itens encontrados")
        
        # Estatísticas resumidas
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("Preço Médio", f"${df_filtrado['SALE_PRICE_USD'].mean():.2f}")
        with col2:
            st.metric("Lucro Médio", f"${df_filtrado['PROFIT'].mean():.2f}")
        with col3:
            st.metric("Margem Média", f"{df_filtrado['MARGIN_PERCENT'].mean():.1f}%")
        
        st.markdown("---")
        
        # Tabela de dados
        st.subheader("📋 Dados Detalhados")
        st.dataframe(
            df_filtrado.style.format({
                'COST_OF_GOODS_USD': '${:.2f}',
                'SALE_PRICE_USD': '${:.2f}',
                'PROFIT': '${:.2f}',
                'MARGIN_PERCENT': '{:.1f}%'
            }),
            use_container_width=True,
            height=400
        )
        
        # Botão de download
        csv = df_filtrado.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="📥 Baixar CSV",
            data=csv,
            file_name=f"tastybytes_filtrado_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv"
        )
    
    # ========================================================================
    # TAB 3: QUERY SQL
    # ========================================================================
    
    with tab3:
        st.header("💻 Execute Queries SQL Personalizadas")
        
        st.markdown("**Exemplos de queries que você pode executar:**")
        
        col1, col2 = st.columns(2)
        
        with col1:
            if st.button("📊 Todos os itens"):
                st.session_state.query = "SELECT * FROM MENU LIMIT 20"
            
            if st.button("💰 Itens mais caros"):
                st.session_state.query = "SELECT MENU_ITEM_NAME, SALE_PRICE_USD FROM MENU ORDER BY SALE_PRICE_USD DESC LIMIT 10"
            
            if st.button("🍰 Apenas sobremesas"):
                st.session_state.query = "SELECT * FROM MENU WHERE ITEM_CATEGORY = 'Dessert'"
        
        with col2:
            if st.button("📈 Contagem por categoria"):
                st.session_state.query = "SELECT ITEM_CATEGORY, COUNT(*) AS TOTAL FROM MENU GROUP BY ITEM_CATEGORY ORDER BY TOTAL DESC"
            
            if st.button("💹 Margem média"):
                st.session_state.query = "SELECT ITEM_CATEGORY, ROUND(AVG((SALE_PRICE_USD - COST_OF_GOODS_USD) / SALE_PRICE_USD * 100), 2) AS AVG_MARGIN FROM MENU GROUP BY ITEM_CATEGORY"
            
            if st.button("🔥 Itens com alta margem"):
                st.session_state.query = "SELECT MENU_ITEM_NAME, ROUND(((SALE_PRICE_USD - COST_OF_GOODS_USD) / SALE_PRICE_USD) * 100, 2) AS MARGIN FROM MENU WHERE ((SALE_PRICE_USD - COST_OF_GOODS_USD) / SALE_PRICE_USD) > 0.7 ORDER BY MARGIN DESC"
        
        st.markdown("---")
        
        # Área de texto para query
        query_text = st.text_area(
            "Digite sua query SQL:",
            value=st.session_state.get('query', "SELECT * FROM MENU LIMIT 10"),
            height=150
        )
        
        if st.button("▶️ Executar Query", type="primary", use_container_width=True):
            try:
                with st.spinner("Executando no Snowflake..."):
                    resultado = run_query(query_text)
                
                st.success(f"✅ Query executada! {len(resultado)} linhas retornadas em {resultado.shape[1]} colunas")
                
                # Mostrar resultado
                st.subheader("📊 Resultado:")
                st.dataframe(resultado, use_container_width=True)
                
                # Download
                csv = resultado.to_csv(index=False).encode('utf-8')
                st.download_button(
                    label="📥 Baixar Resultado CSV",
                    data=csv,
                    file_name=f"query_resultado_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv"
                )
                
            except Exception as e:
                st.error(f"❌ Erro ao executar query:")
                st.code(str(e))
    
    # ========================================================================
    # TAB 4: TUTORIAL
    # ========================================================================
    
    with tab4:
        st.header("📚 Como Foi Feito Este Projeto")
        
        st.markdown("""
        ### 🎯 Objetivo
        
        Este projeto demonstra como integrar **Streamlit** com **Snowflake** 
        para criar dashboards analíticos em tempo real conectados a um data warehouse na nuvem.
        
        ---
        
        ### 🏗️ Arquitetura
        
        ```
        [Usuário] → [Streamlit UI] → [Python] → [Snowflake Connector] → [Snowflake Data Warehouse]
        ```
        
        ---
        
        ### 📦 Tecnologias Utilizadas
        
        - **Streamlit**: Framework para criar apps web em Python
        - **Snowflake**: Data warehouse na nuvem (região São Paulo)
        - **Plotly**: Biblioteca para gráficos interativos
        - **Pandas**: Manipulação de dados
        
        ---
        
        ### 🔧 Configuração do Projeto
        
        **1. Arquivo `.streamlit/secrets.toml`:**
        
        ```toml
        [snowflake]
        user = "seu_usuario"
        password = "sua_senha"
        account = "conta.regiao"
        warehouse = "COMPUTE_WH"
        database = "SNOWFLAKE_LEARNING_DB"
        schema = "seu_schema"
        role = "ACCOUNTADMIN"
        ```
        
        **2. Conexão com Cache (CORRIGIDA):**
        
        ```python
        @st.cache_resource
        def init_connection():
            return snowflake.connector.connect(...)
        
        @st.cache_data(ttl=600)
        def run_query(query):
            conn = init_connection()
            cur = conn.cursor()
            try:
                cur.execute(query)
                return cur.fetch_pandas_all()
            finally:
                cur.close()
        ```
        
        ---
        
        ### ✨ Funcionalidades Implementadas
        
        ✅ Conexão segura com Snowflake  
        ✅ Cache de queries (otimização)  
        ✅ Dashboards interativos  
        ✅ Filtros dinâmicos  
        ✅ Queries SQL personalizadas  
        ✅ Export de dados (CSV)  
        ✅ Visualizações com Plotly  
        
        ---
        
        ### 📊 Sobre os Dados
        
        **TastyBytes** é um dataset fictício da Snowflake que simula 
        dados de uma rede global de food trucks. Contém:
        
        - 🍕 Itens do cardápio
        - 💰 Preços e custos
        - 📈 Margens de lucro
        - 🏷️ Categorias e subcategorias
        
        ---
        
        ### 🎓 Conceitos Importantes
        
        **1. Data Warehouse:**
        - Armazena grandes volumes de dados
        - Otimizado para análises
        - Separação compute/storage
        
        **2. Cache:**
        - `@st.cache_resource`: Cache de conexões (não fecha)
        - `@st.cache_data`: Cache de queries (com TTL)
        - TTL (Time To Live): 600 segundos
        
        **3. Segurança:**
        - Credenciais em `secrets.toml`
        - Nunca commitar senhas no GitHub
        - Usar `.gitignore`
        
        ---
        
        ### 🚀 Como Executar Localmente
        
        ```bash
        # 1. Instalar dependências
        pip install -r requirements.txt
        
        # 2. Configurar credenciais
        # Editar .streamlit/secrets.toml
        
        # 3. Testar conexão
        python teste_conexao.py
        
        # 4. Rodar aplicação
        streamlit run app_snowflake_aula.py
        ```
        
        ---
        
        ### 📚 Recursos para Aprender Mais
        
        - [Documentação Streamlit](https://docs.streamlit.io)
        - [Snowflake Docs](https://docs.snowflake.com)
        - [Plotly Python](https://plotly.com/python/)
        
        ---
        
        ### 👨‍🏫 Créditos
        
        **Professor:** Francisco Fambrini  
        **Instituição:** UNICAMP  
        **Curso:** Ciência de Dados  
        **Ano:** 2025
        """)

# ============================================================================
# FOOTER
# ============================================================================

st.markdown("---")
st.markdown("""
<div style='text-align: center'>
    <p>🎓 <b>Projeto Demonstrativo - UNICAMP</b></p>
    <p>Streamlit + Snowflake Data Warehouse</p>
    <p>Prof. Francisco Fambrini | 2025</p>
</div>
""", unsafe_allow_html=True)
