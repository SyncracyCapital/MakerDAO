import asyncio
from datetime import datetime, timedelta

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

import streamlit as st

from metrics import Metric
from processors import GlassNodeProcessor, DeFiLlamaProcessor, BlockAnalyticaProcessor, MKRBurnProcessor, DuneProcessor
from utils import prepare_data, aggregate_stablecoin_supplies

# App configuration
st.set_page_config(
    page_title="MakerDAO Dashboard",
    page_icon="chart_with_upwards_trend",
    layout="wide",
)

# App Header
markdown = """<h1 style='font-family: Calibri; text-align: center;'><img 
src="https://images.squarespace-cdn.com/content/v1/63857484f91d71181b02f971/9943adcc-5e69-489f-b4a8-158f20fe0619
/Snycracy_WebLogo.png?format=700w" alt="logo"/></h1>"""

st.markdown(markdown, unsafe_allow_html=True)

st.markdown('-------------------')

# Data Sources configuration
base_api_url = 'https://api.glassnode.com/v1/metrics'
api_key = st.secrets['GLASSNODE_API_KEY']
glassnode_processor = GlassNodeProcessor()

# DeFi Llama Data
base_api_url_defillama = 'https://stablecoins.llama.fi'
defillama_processor = DeFiLlamaProcessor()

# Block Analytica
base_api_url_block_analytica = 'https://maker-api.blockanalitica.com'
block_analytica_processor = BlockAnalyticaProcessor()

# MKRBurn Data
base_api_url_mkrburn = 'https://api.makerburn.com'
mkrburn_processor = MKRBurnProcessor()

# Dune Data
base_api_url_dune = 'https://api.dune.com/api/v1'
dune_api_key = st.secrets['DUNE_API_KEY']
dune_processor = DuneProcessor()

# Array of Metrics objects to fetch
metrics = [
    # Glassnode Metrics
    Metric(base_api_url, 'supply', api_key, metric_name='current', params={'a': 'USDT', 'api_key': api_key},
           processor=glassnode_processor, asset_name='USDT', df_col_name='USDT Supply'),
    Metric(base_api_url, 'supply', api_key, metric_name='current', params={'a': 'USDC', 'api_key': api_key},
           processor=glassnode_processor, asset_name='USDC', df_col_name='USDC Supply'),
    Metric(base_api_url, 'supply', api_key, metric_name='current', params={'a': 'TUSD', 'api_key': api_key},
           processor=glassnode_processor, asset_name='TUSD', df_col_name='TUSD Supply'),
    Metric(base_api_url, 'supply', api_key, metric_name='current', params={'a': 'BUSD', 'api_key': api_key},
           processor=glassnode_processor, asset_name='BUSD', df_col_name='BUSD Supply'),
    Metric(base_api_url, 'supply', api_key, metric_name='current', params={'a': 'GUSD', 'api_key': api_key},
           processor=glassnode_processor, asset_name='GUSD', df_col_name='GUSD Supply'),
    Metric(base_api_url, 'supply', api_key, metric_name='current', params={'a': 'DAI', 'api_key': api_key},
           processor=glassnode_processor, asset_name='DAI', df_col_name='DAI Supply'),
    Metric(base_api_url, 'supply', api_key, metric_name='current', params={'a': 'FRAX', 'api_key': api_key},
           processor=glassnode_processor, asset_name='FRAX', df_col_name='FRAX Supply'),

    # # DeFi Llama Metrics
    # # DAI DeFi Llama ID is 5
    Metric(base_api_url_defillama, 'stablecoin', api_key, metric_name='5',
           processor=defillama_processor, asset_name='DAI', df_col_name='DAI Supply (DeFi Llama)'),
    # # crvUSD DeFi Llama ID is 110
    Metric(base_api_url_defillama, 'stablecoin', api_key, metric_name='110',
           processor=defillama_processor, asset_name='crvUSD', df_col_name='crvUSD Supply (DeFi Llama)'),
    # FRAX DeFi Llama ID is 6
    Metric(base_api_url_defillama, 'stablecoin', api_key, metric_name='6',
           processor=defillama_processor, asset_name='FRAX', df_col_name='FRAX Supply (DeFi Llama)'),
    # LUSD DeFi Llama ID is 8
    Metric(base_api_url_defillama, 'stablecoin', api_key, metric_name='8',
           processor=defillama_processor, asset_name='LUSD', df_col_name='LUSD Supply (DeFi Llama)'),
    # MIM DeFi Llama ID is 10
    Metric(base_api_url_defillama, 'stablecoin', api_key, metric_name='10',
           processor=defillama_processor, asset_name='MIM', df_col_name='MIM Supply (DeFi Llama)'),
    # FEI DeFi Llama ID is 9
    Metric(base_api_url_defillama, 'stablecoin', api_key, metric_name='9',
           processor=defillama_processor, asset_name='FEI', df_col_name='FEI Supply (DeFi Llama)'),

    # Block Analytica Metrics
    Metric(base_api_url_block_analytica, 'risk', api_key, metric_name='liquidation-curve/',
           params={'format': 'json'}, processor=block_analytica_processor, asset_name='N/A',
           df_col_name='Debt-at-Risk'),
    Metric(base_api_url_block_analytica, 'psms', api_key, metric_name='dai-supply-history/',
           params={'days_ago': '90', 'format': 'json'}, processor=block_analytica_processor, asset_name='N/A',
           df_col_name='PSMS'),

    # MKRBurn Metrics
    Metric(base_api_url_mkrburn, 'history', api_key,
           processor=mkrburn_processor, asset_name='Surplus Buffer', df_col_name='Surplus Buffer'),
    Metric(base_api_url_mkrburn, 'treasury', api_key,
           processor=mkrburn_processor, asset_name='Treasury', df_col_name='Treasury'),

    # Dune Metrics

    # Where is my DAI?
    Metric(base_api_url_dune, 'query', api_key, metric_name='3059618/results',
           params={'api_key': dune_api_key}, processor=dune_processor, df_col_name='Where is my DAI?'),
    # MKR Annualized Revenue
    Metric(base_api_url_dune, 'query', api_key, metric_name='3059627/results',
           params={'api_key': dune_api_key}, processor=dune_processor, df_col_name='Annualized MKR Revenue'),
    # PSM Statistics
    Metric(base_api_url_dune, 'query', api_key, metric_name='3059668/results',
           params={'api_key': dune_api_key}, processor=dune_processor, df_col_name='PSM Statistics'),
]

with st.spinner('Fetching data from APIs...'):
    data_dict = asyncio.run(prepare_data(metrics))

start_date, end_date, _, _, _, _, _ = st.columns(7)

with start_date:
    zoom_in_date_start = st.date_input('Start Date', datetime.today() - timedelta(days=365))

with end_date:
    zoom_in_date_end = st.date_input('End Date', datetime.today())

st.markdown('---')

st.header('DAI Metrics')

total_stable_coin_supply, dai_pct_penetration, dai_supply_across_chains = st.columns(3)

distance_from_plot = 0.90
SYNCRACY_COLORS = ['#5218F8', '#F8184E', '#C218F8']

with total_stable_coin_supply:
    df = aggregate_stablecoin_supplies(data_dict)
    data_subset = df.loc[zoom_in_date_start:zoom_in_date_end]

    min_val = 0
    max_val = data_subset.sum(axis=1).max()

    fig = px.area(df, title='Total Stablecoin Supply')
    fig.update_layout(xaxis_title=None, yaxis_title=None)

    fig.update_xaxes(type="date", range=[zoom_in_date_start, zoom_in_date_end])
    fig.update_yaxes(range=[min_val, max_val])

    fig.update_layout(
        title={
            'y': distance_from_plot,
            'x': 0,
            'xanchor': 'left',
            'yanchor': 'top'},
        legend_title_text=''
    )

    fig.update_layout(hovermode="x unified")
    st.plotly_chart(fig, use_container_width=True)

    st.download_button(label="Download Data", data=df.to_csv(), file_name='total_stablecoin_supply.csv',
                       mime='text/csv')

with dai_pct_penetration:
    df = aggregate_stablecoin_supplies(data_dict)
    df = df.divide(df.sum(axis=1), axis=0)
    df['DAI Supply'] = df['DAI Supply'].rolling(7).mean()

    fig = px.line(df, x=df.index, y=df['DAI Supply'], title='Total Stablecoin Supply: DAI % Penetration')
    fig.update_traces(line=dict(color="#5218fa"))
    fig.update_layout(showlegend=False, xaxis_title=None, yaxis_title=None)

    data_subset = df.loc[zoom_in_date_start:zoom_in_date_end]

    min_val = data_subset['DAI Supply'].min()
    max_val = data_subset['DAI Supply'].max()

    fig.update_xaxes(type="date", range=[zoom_in_date_start, zoom_in_date_end])
    fig.update_yaxes(range=[min_val, max_val])
    fig.update_yaxes(tickformat=".2%")

    fig.update_layout(
        title={
            'y': distance_from_plot,
            'x': 0,
            'xanchor': 'left',
            'yanchor': 'top'}
    )

    fig.update_layout(hovermode="x unified")
    st.plotly_chart(fig, use_container_width=True)

    st.download_button(label="Download Data", data=df.to_csv(), file_name='dai_pct_penetration.csv', mime='text/csv')

with dai_supply_across_chains:
    df = data_dict['DAI Supply (DeFi Llama)']
    df.drop('Total_circulating_supply', axis=1, inplace=True)
    df_normalized = df.divide(df.sum(axis=1), axis=0)

    dai_pct_share_threshold = 0.005
    major_chains = df_normalized.columns[df_normalized.iloc[-1] > dai_pct_share_threshold].tolist()
    minor_chains = df_normalized.columns[df_normalized.iloc[-1] <= dai_pct_share_threshold].tolist()
    df['Other'] = df[minor_chains].sum(axis=1)
    df = df[major_chains + ['Other']]
    df.columns = [col.split('_')[0] for col in df.columns]

    data_subset = df.loc[zoom_in_date_start:zoom_in_date_end]

    min_val = 0
    max_val = data_subset.sum(axis=1).max()

    fig = px.area(df, title='DAI Supply Across Chains')
    fig.update_layout(xaxis_title=None, yaxis_title=None)

    fig.update_xaxes(type="date", range=[zoom_in_date_start, zoom_in_date_end])
    fig.update_yaxes(range=[min_val, max_val])

    fig.update_layout(
        title={
            'y': distance_from_plot,
            'x': 0,
            'xanchor': 'left',
            'yanchor': 'top'},
        legend_title_text=''
    )

    fig.update_layout(hovermode="x unified")
    st.plotly_chart(fig, use_container_width=True)

    st.download_button(label="Download Data", data=df.to_csv(), file_name='dai_supply_across_chains.csv',
                       mime='text/csv')

total_decentralized_stablecoin_supply, dai_pct_penetration_decentralized, where_is_my_dai = st.columns(3)

with total_decentralized_stablecoin_supply:
    df = aggregate_stablecoin_supplies(data_dict)
    df = df[['DAI Supply', 'FRAX Supply', 'crvUSD Supply', 'LUSD Supply', 'MIM Supply', 'FEI Supply']]

    data_subset = df.loc[zoom_in_date_start:zoom_in_date_end]

    min_val = 0
    max_val = data_subset.sum(axis=1).max()

    fig = px.area(df, title='Total Decentralized Stablecoin Supply')
    fig.update_layout(xaxis_title=None, yaxis_title=None)

    fig.update_xaxes(type="date", range=[zoom_in_date_start, zoom_in_date_end])
    fig.update_yaxes(range=[min_val, max_val])

    fig.update_layout(
        title={
            'y': distance_from_plot,
            'x': 0,
            'xanchor': 'left',
            'yanchor': 'top'},
        legend_title_text=''
    )

    fig.update_layout(hovermode="x unified")
    st.plotly_chart(fig, use_container_width=True)

    st.download_button(label="Download Data", data=df.to_csv(), file_name='total_decentralized_stablecoin_supply.csv',
                       mime='text/csv')

with dai_pct_penetration_decentralized:
    df = aggregate_stablecoin_supplies(data_dict)
    df = df[['DAI Supply', 'FRAX Supply', 'crvUSD Supply', 'LUSD Supply', 'MIM Supply', 'FEI Supply']]
    df = df.divide(df.sum(axis=1), axis=0)
    df['DAI Supply'] = df['DAI Supply'].rolling(7).mean()

    fig = px.line(df, x=df.index, y=df['DAI Supply'], title='Total Decentralized Stablecoin Supply: DAI % Penetration')
    fig.update_traces(line=dict(color="#5218fa"))
    fig.update_layout(showlegend=False, xaxis_title=None, yaxis_title=None)

    data_subset = df.loc[zoom_in_date_start:zoom_in_date_end]

    min_val = data_subset['DAI Supply'].min()
    max_val = data_subset['DAI Supply'].max()

    fig.update_xaxes(type="date", range=[zoom_in_date_start, zoom_in_date_end])
    fig.update_yaxes(range=[min_val, max_val])
    fig.update_yaxes(tickformat=".0%")

    fig.update_layout(
        title={
            'y': distance_from_plot,
            'x': 0,
            'xanchor': 'left',
            'yanchor': 'top'}
    )

    fig.update_layout(hovermode="x unified")
    st.plotly_chart(fig, use_container_width=True)

    st.download_button(label="Download Data", data=df.to_csv(), file_name='dai_pct_penetration_decentralized.csv',
                       mime='text/csv')

with where_is_my_dai:
    df = data_dict['Where is my DAI?']
    df.reset_index(inplace=True)
    df_pivot = df.pivot(index='index', columns='wallet', values='balance').fillna(0)
    df_pivot = df_pivot.divide(df_pivot.sum(axis=1), axis=0)

    data_subset = df_pivot.loc[zoom_in_date_start:zoom_in_date_end]

    min_val = 0
    max_val = data_subset.sum(axis=1).max()

    fig = px.area(df_pivot, title='Where is my DAI? (Relative)')
    fig.update_layout(xaxis_title=None, yaxis_title=None)

    fig.update_xaxes(type="date", range=[zoom_in_date_start, zoom_in_date_end])
    fig.update_yaxes(range=[min_val, max_val])
    fig.update_yaxes(tickformat=".0%")

    fig.update_layout(
        title={
            'y': distance_from_plot,
            'x': 0,
            'xanchor': 'left',
            'yanchor': 'top'},
        legend_title_text=''
    )

    fig.update_layout(hovermode="x unified")
    st.plotly_chart(fig, use_container_width=True)

    st.download_button(label="Download Data", data=df.to_csv(), file_name='where_is_my_dai.csv',
                       mime='text/csv')

where_is_my_dai_abs, _, _ = st.columns(3)

with where_is_my_dai_abs:
    df = data_dict['Where is my DAI?']
    df.reset_index(inplace=True)
    df_pivot = df.pivot(index='index', columns='wallet', values='balance').fillna(0)

    data_subset = df_pivot.loc[zoom_in_date_start:zoom_in_date_end]

    min_val = 0
    max_val = data_subset.sum(axis=1).max()

    fig = px.area(df_pivot, title='Where is my DAI? (Absolute)')
    fig.update_layout(xaxis_title=None, yaxis_title=None)

    fig.update_xaxes(type="date", range=[zoom_in_date_start, zoom_in_date_end])
    fig.update_yaxes(range=[min_val, max_val])

    fig.update_layout(
        title={
            'y': distance_from_plot,
            'x': 0,
            'xanchor': 'left',
            'yanchor': 'top'},
        legend_title_text=''
    )

    fig.update_layout(hovermode="x unified")
    st.plotly_chart(fig, use_container_width=True)

    st.download_button(label="Download Data", data=df.to_csv(), file_name='where_is_my_dai_abs.csv',
                       mime='text/csv')

st.header('Maker Specific Metrics')

debt_breakdown, psm_reserves, psm_swap_fees = st.columns(3)

with debt_breakdown:
    df = data_dict['Debt-at-Risk']
    df['drop'] = df['drop'] / 100
    df_pivot = df.pivot_table(index='drop', columns='protection_score', values='debt', aggfunc='sum').fillna(0)
    col_order = ['low', 'medium', 'high']
    df_pivot = df_pivot[col_order]

    color_map = {
        'low': 'green',
        'medium': 'yellow',
        'high': 'red'
    }

    fig = px.area(df_pivot, title='Debt-at-Risk', color_discrete_map=color_map)
    fig.update_layout(xaxis_title='Price Drop', yaxis_title='Debt-at-Risk')

    fig.update_xaxes(tickformat=".0%")

    fig.update_layout(
        title={
            'y': distance_from_plot,
            'x': 0,
            'xanchor': 'left',
            'yanchor': 'top'},
        legend_title_text=''
    )

    fig.update_layout(hovermode="x unified")
    st.plotly_chart(fig, use_container_width=True)

    st.download_button(label="Download Data", data=df.to_csv(), file_name='debt_at_risk.csv',
                       mime='text/csv')

with psm_reserves:
    df = data_dict['PSM Statistics']
    cols_to_keep = ['psm_balance', 'inflow', 'outflow']
    df = df[cols_to_keep]

    data_subset = df.loc[zoom_in_date_start:zoom_in_date_end]

    min_val = 0
    max_val = data_subset.sum(axis=1).max()

    fig = go.Figure()

    fig.add_trace(go.Scatter(x=df.index, y=df['psm_balance'], mode='lines', name='PSM Balance'))

    fig.add_trace(go.Bar(x=df.index, y=df['outflow'], name='Outflows', yaxis='y2'))
    fig.add_trace(go.Bar(x=df.index, y=df['inflow'], name='Inflows', yaxis='y2'))

    fig.update_xaxes(type="date", range=[zoom_in_date_start, zoom_in_date_end])
    fig.update_yaxes(range=[min_val, max_val])

    fig.update_layout(
        yaxis=dict(title='PSM Balance'),
        yaxis2=dict(title='Flows', overlaying='y', side='right'),
        barmode='stack'
    )

    fig.update_layout(hovermode="x unified")
    fig.update_layout(title_text='PSM: Volume and Balance')

    st.plotly_chart(fig, use_container_width=True)

    st.download_button(label="Download Data", data=df.to_csv(), file_name='psm_stats.csv',
                       mime='text/csv')

with psm_swap_fees:
    df = data_dict['PSM Statistics']
    cols_to_keep = ['lifetime_fees', 'fees']
    df = df[cols_to_keep]

    data_subset = df.loc[zoom_in_date_start:zoom_in_date_end]

    min_val = 0
    max_val = data_subset.sum(axis=1).max()

    fig = go.Figure()

    fig.add_trace(go.Scatter(x=df.index, y=df['lifetime_fees'], mode='lines', name='Cumulative Fees'))

    fig.add_trace(go.Bar(x=df.index, y=df['fees'], name='Fees', yaxis='y2'))

    fig.update_xaxes(type="date", range=[zoom_in_date_start, zoom_in_date_end])
    fig.update_yaxes(range=[min_val, max_val])

    fig.update_layout(
        yaxis=dict(title='PSM Cumulative Fees'),
        yaxis2=dict(title='Fees', overlaying='y', side='right'),
    )

    fig.update_layout(hovermode="x unified")
    fig.update_layout(title_text='PSM: Swap Fees')

    st.plotly_chart(fig, use_container_width=True)

    st.download_button(label="Download Data", data=df.to_csv(), file_name='psm_fees.csv',
                       mime='text/csv')

surplus_buffer, revenue_breakdown, collateral_by_type = st.columns(3)

with surplus_buffer:
    df = data_dict['Surplus Buffer']
    cols_to_keep = ['surplus']
    df = df[cols_to_keep]

    fig = px.line(df, x=df.index, y=df['surplus'], title='Surplus Buffer')
    fig.update_traces(line=dict(color="#5218fa"))
    fig.update_layout(showlegend=False, xaxis_title=None, yaxis_title=None)

    data_subset = df.loc[zoom_in_date_start:zoom_in_date_end]

    min_val = data_subset['surplus'].min()
    max_val = data_subset['surplus'].max()

    fig.update_xaxes(type="date", range=[zoom_in_date_start, zoom_in_date_end])
    fig.update_yaxes(range=[min_val, max_val])

    fig.update_layout(
        title={
            'y': distance_from_plot,
            'x': 0,
            'xanchor': 'left',
            'yanchor': 'top'}
    )

    fig.update_layout(hovermode="x unified")
    st.plotly_chart(fig, use_container_width=True)

    st.download_button(label="Download Data", data=df.to_csv(), file_name='surplus_buffer.csv',
                       mime='text/csv')

with revenue_breakdown:
    df = data_dict['Annualized MKR Revenue']
    df.reset_index(inplace=True)
    df_pivot = df.pivot_table(index='index', columns='collateral', values='annual_revenues').fillna(0)

    data_subset = df_pivot.loc[zoom_in_date_start:zoom_in_date_end]

    min_val = 0
    max_val = data_subset.sum(axis=1).max()

    fig = px.area(df_pivot, title='MKR Revenue by Type (Annualized)')
    fig.update_layout(xaxis_title=None, yaxis_title=None)

    fig.update_xaxes(type="date", range=[zoom_in_date_start, zoom_in_date_end])
    fig.update_yaxes(range=[min_val, max_val])

    fig.update_layout(
        title={
            'y': distance_from_plot,
            'x': 0,
            'xanchor': 'left',
            'yanchor': 'top'},
        legend_title_text=''
    )

    fig.update_layout(hovermode="x unified")
    st.plotly_chart(fig, use_container_width=True)

    st.download_button(label="Download Data", data=df.to_csv(), file_name='mkr_revenue.csv',
                       mime='text/csv')

with collateral_by_type:
    df = data_dict['Annualized MKR Revenue']
    df.reset_index(inplace=True)
    df_pivot = df.pivot_table(index='index', columns='collateral', values='asset').fillna(0)

    data_subset = df_pivot.loc[zoom_in_date_start:zoom_in_date_end]

    min_val = 0
    max_val = data_subset.sum(axis=1).max()

    fig = px.area(df_pivot, title='MKR Collateral by Type')
    fig.update_layout(xaxis_title=None, yaxis_title=None)

    fig.update_xaxes(type="date", range=[zoom_in_date_start, zoom_in_date_end])
    fig.update_yaxes(range=[min_val, max_val])

    fig.update_layout(
        title={
            'y': distance_from_plot,
            'x': 0,
            'xanchor': 'left',
            'yanchor': 'top'},
        legend_title_text=''
    )

    fig.update_layout(hovermode="x unified")
    st.plotly_chart(fig, use_container_width=True)

    st.download_button(label="Download Data", data=df.to_csv(), file_name='mkr_collateral.csv',
                       mime='text/csv')

mkr_treasury, _, _ = st.columns(3)

with mkr_treasury:
    df = data_dict['Treasury']
    df = df[['dai_balance', 'system_surplus', 'MKR Balance', 'AAVE Balance', 'ENS Balance']]
    df.columns = ['DAI Balance', 'System Surplus', 'MKR Balance', 'AAVE Balance', 'ENS Balance']

    data_subset = df.loc[zoom_in_date_start:zoom_in_date_end]

    min_val = 0
    max_val = data_subset.sum(axis=1).max()

    fig = px.area(df, title='MKR Treasury')
    fig.update_layout(xaxis_title=None, yaxis_title=None)

    fig.update_xaxes(type="date", range=[zoom_in_date_start, zoom_in_date_end])
    fig.update_yaxes(range=[min_val, max_val])

    fig.update_layout(
        title={
            'y': distance_from_plot,
            'x': 0,
            'xanchor': 'left',
            'yanchor': 'top'},
        legend_title_text=''
    )

    fig.update_layout(hovermode="x unified")
    st.plotly_chart(fig, use_container_width=True)

    st.download_button(label="Download Data", data=df.to_csv(), file_name='mkr_treasury.csv',
                       mime='text/csv')
