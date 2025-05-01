import pandas as pd
import plotly.graph_objects as go
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def transform_data():
    try:
        # Load raw data
        df = pd.read_csv("raw_btc_usd.csv")
        logging.info("Transforming data...")

        # Clean data
        df['time'] = pd.to_datetime(df['time'])
        df['ReferenceRateUSD'] = df['ReferenceRateUSD'].astype(float)
        df = df.dropna(subset=['ReferenceRateUSD'])  # Remove missing prices
        df = df.sort_values('time')  # Ensure chronological order
        df = df.set_index('time')  # Set date as index

        # Save cleaned data
        df.to_csv("transformed_btc_usd.csv")
        logging.info(f"Cleaned data saved with {len(df)} records")

        # Create Plotly chart
        fig = go.Figure()
        fig.add_trace(
            go.Scatter(
                x=df.index,
                y=df['ReferenceRateUSD'],
                mode='lines',
                name='BTC/USD',
                line=dict(color='orange', width=2)
            )
        )

        # Customize layout
        fig.update_layout(
            title='BTC/USD Price (Daily)',
            xaxis_title='Date',
            yaxis_title='Price (USD)',
            template='plotly_dark',  # Black background
            plot_bgcolor='black',
            paper_bgcolor='black',
            font=dict(color='white'),
            xaxis=dict(gridcolor='gray'),
            yaxis=dict(gridcolor='gray')
        )

        # Save chart as HTML
        fig.write_html("btc_usd_chart.html", auto_open=False)
        logging.info("Plotly chart saved as btc_usd_chart.html")
    except Exception as e:
        logging.error(f"Transformation failed: {str(e)}")
        raise

if __name__ == "__main__":
    transform_data()
