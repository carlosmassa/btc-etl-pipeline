import pandas as pd
import plotly.graph_objects as go
import os
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def transform_data():
    try:
        # Load raw data
        df = pd.read_csv("raw_btc_usd.csv")
        logging.info("Transforming data...")

        # Clean data
        df['time'] = pd.to_datetime(df['time'])
        df['PriceUSD'] = df['PriceUSD'].astype(float)
        df = df.dropna(subset=['PriceUSD'])  # Remove missing prices
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
                y=df['PriceUSD'],
                mode='lines',
                name='BTC/USD',
                line=dict(color='orange', width=2)
            )
        )

        # Anotations
        annotations = []
        annotations.append(dict(x=1, y=-0.1,
            text="Chart by: @CarlesMassa",
            showarrow=False, xref='paper', yref='paper',
            xanchor='right', yanchor='auto', xshift=0, yshift=0))

        annotations.append(dict(
            x=1,              # Position annotation at the far right
            y=-0.15,          # Slightly lower than the legend (adjust as needed)
            text="Chart by: @CarlesMassa",  # Annotation text
            showarrow=False, 
            xref='paper', 
            yref='paper',     # Use paper coordinates to position annotation
            xanchor='right',  # Align annotation to the right of the paper
            yanchor='auto',   # Auto-anchor to adjust automatically
            xshift=0, 
            yshift=0          # Fine-tune vertical positioning
        ))
        
        # Customize layout
        fig.update_layout(
            title='BTC/USD Price (Daily)',
            xaxis_title='Date',
            yaxis_title='Price (USD)',
            yaxis_type='log',
            hovermode='closest',
            template='plotly_dark',  # Black background
            plot_bgcolor='black',
            paper_bgcolor='black',
            showlegend=True,
            legend=dict(
                x=0,  # Left position (adjust as needed)
                y=0,  # Bottom position (for horizontal legend)
                xanchor='left',
                yanchor='bottom',
                orientation="h"  # Horizontal orientation
            ),
            annotations=annotations,  # Your annotations
        )

        fig.update_yaxes(showgrid=False)

        # Define configuration for mode bar buttons
        config = {
            'modeBarButtonsToAdd': [
                'drawline',
                'drawopenpath',
                'drawcircle',
                'drawrect',
                'eraseshape',
                'v1hovermode',
                'togglespikelines'
            ]
        }

        # Save chart as HTML with config
        fig.write_html("btc_usd_chart.html", auto_open=False, config=config)
        # Chart URL: https://carlosmassa.github.io/btc-etl-pipeline/charts/btc_usd_chart.html
        logging.info("Plotly chart saved as btc_usd_chart.html with custom config")

        # Save chart as HD JPG
        os.makedirs("charts", exist_ok=True)
        jpg_path = "charts/btc_usd_chart.jpg"
        fig.write_image(jpg_path, width=1600, height=900, scale=2)
        logging.info(f"HD JPG chart saved as {jpg_path}")
    except Exception as e:
        logging.error(f"Transformation failed: {str(e)}")
        raise

if __name__ == "__main__":
    transform_data()
