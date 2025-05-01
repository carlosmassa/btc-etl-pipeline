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
        print(df)
        # Clean data
        df['Date'] = pd.to_datetime(df['time'])
        df['PriceUSD'] = df['PriceUSD'].astype(float)
        #df = df.dropna(subset=['PriceUSD'])  # Remove missing prices
        df = df.sort_values('Date')  # Ensure chronological order
        df = df.set_index('Date')  # Set date as index



        # fix date formats and set the data as the index
        btc_data = df.reset_index().rename(columns={df.index.name:'Date'})
        btc_data['Date'] = btc_data.Date.apply(lambda x: x.date()) # where dates is your datetime column
        btc_data['Date'] = pd.to_datetime(btc_data['Date'], format='%Y-%m-%d')
        btc_data['Value'] = btc_data['PriceUSD']
        btc_data = btc_data.drop(['PriceUSD'], axis=1)


        btc_data.reset_index()
        df = btc_data.reset_index(drop=True)
        df
        #df = pd.read_csv("C:\\Users\\Aman\\Desktop\\Aman\\crypto_finance\\BTC_USD.csv")[["Date", "Value"]]
        df = btc_data.reset_index()
        df.Date = pd.to_datetime(df.Date)
        df.sort_values(by="Date", inplace = True)
        genesis = pd.to_datetime("2009-01-03")
        print(df)
        print("genesis Date:", genesis)
        #Genesis block is 3rd Jan, 2009, above value is actually 2nd Jan
        df = df[df.Date >= "2010-07-18"]    #useful data from exchanges from 2010 onwards
        df=df.reset_index(drop=True)
        df=df.drop("index", axis=1)
        #delta = (df.Date[0] - genesis).days - 1
        delta = (df.Date[0] - genesis).days
        #There are 561 days between 2009-01-03 and 2010-07-18.
        print(df.Date[0])
        print(df.Date[0], genesis)
        print((df.Date[0] - genesis).days)
        df["ind"] = [x+delta for x in range(len(df))]
        print(df) #index represents number of days since genesis block


        # Save cleaned data
        df.to_csv("transformed_btc_usd.csv")
        logging.info(f"Cleaned data saved with {len(df)} records")




        ######################################################################################################################################
        ######################################################################################################################################
        ######################################################################################################################################

        # Prepare the data for quantile regression (log-log transformation)
        X = np.log(df.ind)  # Log of independent variable (days since genesis)
        y = np.log(df.Value)  # Log of dependent variable (Bitcoin price)
        
        # Add a constant to the independent variable for the intercept in statsmodels
        X_with_const = sm.add_constant(X)
        
        # Define the quantiles of interest
        quantiles = [0.001, 0.05, 0.50, 0.90, 0.98, 0.999]  # Ensure correct formatting
        quantile_labels = ['0.1%', '5%', '50%', '90%', '98%', '99.9%']
        
        # Create a dictionary to store the results for each quantile
        quant_reg_results = {}
        
        # Perform quantile regression for each quantile and store the results
        for q in quantiles:
            quant_reg = sm.QuantReg(y, X_with_const)
            quant_reg_results[q] = quant_reg.fit(q=q)
        
        # Print the summary for 50% quantile (median)
        print(quant_reg_results[0.5].summary())
        
        # Print the summary for 5% quantile (median)
        print(quant_reg_results[0.05].summary())
        
        # Print the summary for 1% quantile (median)
        print(quant_reg_results[0.001].summary())
        
        # Make predictions and convert back to the original scale (antilog)
        for q, label in zip(quantiles, quantile_labels):
            df[f'QuantRegPredict_{label}'] = quant_reg_results[q].predict(X_with_const)
            df[f'LinearReg_{label}'] = np.exp(df[f'QuantRegPredict_{label}'])
        
        # Now use the custom chart_utils function to visualize the results on a linear scale
        # First, plot the actual data and all quantile regression lines
        
        chart_utils.single_axis_chart_special(
            df, x_series='X',
            y1_series=['Value'] + [f'LinearReg_{label}' for label in quantile_labels],
            y1_series_axis_type='log',
            y1_series_axis_range=[-2, 6],
            title="Price Quantile Regression (0.1%, 5%, 50%, 90%, 98%, 99.9%)")
        
        chart_utils.single_axis_chart_special(
            df, x_series='ind',
            y1_series=['Value'] + [f'LinearReg_{label}' for label in quantile_labels],
            y1_series_axis_type='log',
            y1_series_axis_range=[-2, 6],
            title="Price Quantile Regression (0.1%, 5%, 50%, 90%, 98%, 99.9%)")
        
        chart_utils.single_axis_chart_special(
            df, x_series='Date',
            y1_series=['Value'] + [f'LinearReg_{label}' for label in quantile_labels],
            y1_series_axis_type='log',
            y1_series_axis_range=[-2, 6],
            title="Price Quantile Regression (0.1%, 5%, 50%, 90%, 98%, 99.9%)")
        
        # Coefficient and intercept for the 50% quantile (as an example)
        print(f"Coefficient (50% quantile): {quant_reg_results[0.5].params[1]}")
        print(f"Intercept (50% quantile): {quant_reg_results[0.5].params[0]}")
        
        # The quantile regression equation for the 50% quantile
        print(f"Quantile regression equation (50%): Predicted Price = e^({quant_reg_results[0.5].params[1]} * ln(number of days since genesis block) + {quant_reg_results[0.5].params[0]})")
        
        # Coefficient and intercept for the 50% quantile (as an example)
        print(f"Coefficient (5% quantile): {quant_reg_results[0.05].params[1]}")
        print(f"Intercept (5% quantile): {quant_reg_results[0.05].params[0]}")
        # The quantile regression equation for the 5% quantile
        print(f"Quantile regression equation (5%): Predicted Price = e^({quant_reg_results[0.05].params[1]} * ln(number of days since genesis block) + {quant_reg_results[0.05].params[0]})")
        
        # Coefficient and intercept for the 50% quantile (as an example)
        print(f"Coefficient (1% quantile): {quant_reg_results[0.001].params[1]}")
        print(f"Intercept (1% quantile): {quant_reg_results[0.001].params[0]}")
        # The quantile regression equation for the 1% quantile
        print(f"Quantile regression equation (1%): Predicted Price = e^({quant_reg_results[0.001].params[1]} * ln(number of days since genesis block) + {quant_reg_results[0.001].params[0]})")
        
        # Calculate and print the Mean Squared Error for the 50% quantile
        mse_50_percent = mean_squared_error(y, quant_reg_results[0.5].predict(X_with_const))
        print(f"Mean squared error (50% quantile): {mse_50_percent:.2f}")
        
        # Print the pseudo R-squared for the 50% quantile regression
        print(f"Pseudo R-squared (50% quantile): {quant_reg_results[0.5].prsquared:.2f}")
        
        # Calculate and print the Mean Squared Error for the 5% quantile
        mse_50_percent = mean_squared_error(y, quant_reg_results[0.05].predict(X_with_const))
        print(f"Mean squared error (5% quantile): {mse_50_percent:.2f}")
        
        # Print the pseudo R-squared for the 5% quantile regression
        print(f"Pseudo R-squared (5% quantile): {quant_reg_results[0.05].prsquared:.2f}")
        
        # Calculate and print the Mean Squared Error for the 1% quantile
        mse_50_percent = mean_squared_error(y, quant_reg_results[0.001].predict(X_with_const))
        print(f"Mean squared error (1% quantile): {mse_50_percent:.2f}")
        
        # Print the pseudo R-squared for the 1% quantile regression
        print(f"Pseudo R-squared (1% quantile): {quant_reg_results[0.001].prsquared:.2f}")
        
        
        ###################################################################################################################
        ###################################################################################################################
        ###################################################################################################################
        # === STEP 1: Extend 5 years into the future ===
        future_dates = pd.date_range(start=df['Date'].max() + pd.Timedelta(days=1), periods=5*365, freq='D')
        future_df = pd.DataFrame({'Date': future_dates})
        future_df['ind'] = (future_df['Date'] - genesis).dt.days
        
        # === STEP 2: Prepare log(ind) + const for predictions ===
        X_future_log = np.log(future_df['ind'])
        X_future_with_const = sm.add_constant(X_future_log)
        
        # === STEP 3: Predict future values using fitted quantile regression models ===
        for q, label in zip(quantiles, quantile_labels):
            future_df[f'QuantRegPredict_{label}'] = quant_reg_results[q].predict(X_future_with_const)
        
            # Use the label exactly as it is for LinearReg columns
            future_df[f'LinearReg_{label}'] = np.exp(future_df[f'QuantRegPredict_{label}'])
        
        # === STEP 4: Append to main df for Plotly chart ===
        combined_df = pd.concat([df, future_df], ignore_index=True)
        
        ###################################################################################################################
        ###################################################################################################################
        ###################################################################################################################
        
        
        
        # Create the Plotly figure with a black background
        fig = go.Figure()
        
        # Add traces for the price and 50% quantile
        fig.add_trace(go.Scatter(x=df['Date'], y=df['Value'], mode='lines', name='Price', line=dict(color='orange', width=3)))
        
        fig.add_trace(go.Scatter(x=combined_df['Date'], y=combined_df['LinearReg_50%'], mode='lines', name='50% Quantile', line=dict(color='cyan', width=2)))
        fig.add_trace(go.Scatter(x=combined_df['Date'], y=combined_df['LinearReg_0.1%'], fill=None, mode='lines', line=dict(color='#A8D800', width=1), name='1% Quantile', showlegend=False))
        fig.add_trace(go.Scatter(x=combined_df['Date'], y=combined_df['LinearReg_5%'], fill='tonexty', mode='lines', line=dict(color='#00FF7F', width=0), name='5% Quantile', showlegend=False))
        fig.add_trace(go.Scatter(x=combined_df['Date'], y=combined_df['LinearReg_90%'], fill=None, mode='lines', line=dict(color='#00BFFF', width=1), name='90% Quantile', showlegend=False))
        fig.add_trace(go.Scatter(x=combined_df['Date'], y=combined_df['LinearReg_98%'], fill='tonexty', mode='lines', line=dict(color='#87CEFA', width=0), name='98% Quantile', showlegend=False))
        fig.add_trace(go.Scatter(x=combined_df['Date'], y=combined_df['LinearReg_98%'], fill=None, mode='lines', line=dict(color='#FF6347', width=1), name='98% Quantile', showlegend=False))
        fig.add_trace(go.Scatter(x=combined_df['Date'], y=combined_df['LinearReg_99.9%'], fill='tonexty', mode='lines', line=dict(color='#FF4500', width=0), name='99.9% Quantile', showlegend=False))
        
        
        
        #fig.add_trace(go.Scatter(x=df['Date'], y=df['LinearReg_50%'], mode='lines', name='50% Quantile', line=dict(color='cyan', width=2)))
        
        # Add the bands
        # Band between 1% and 5%
        #fig.add_trace(go.Scatter(x=df['Date'], y=df['LinearReg_0.1%'], fill=None, mode='lines', line=dict(color='#A8D800', width=1), name='1% Quantile', showlegend=False))
        #fig.add_trace(go.Scatter(x=df['Date'], y=df['LinearReg_5%'], fill='tonexty', mode='lines', line=dict(color='#00FF7F', width=0), name='5% Quantile', showlegend=False))
        
        # Band between 90% and 98%
        #fig.add_trace(go.Scatter(x=df['Date'], y=df['LinearReg_90%'], fill=None, mode='lines', line=dict(color='#00BFFF', width=1), name='90% Quantile', showlegend=False))
        #fig.add_trace(go.Scatter(x=df['Date'], y=df['LinearReg_98%'], fill='tonexty', mode='lines', line=dict(color='#87CEFA', width=0), name='98% Quantile', showlegend=False))
        
        # Band between 98% and 99.9%
        #fig.add_trace(go.Scatter(x=df['Date'], y=df['LinearReg_98%'], fill=None, mode='lines', line=dict(color='#FF6347', width=1), name='98% Quantile', showlegend=False))
        #fig.add_trace(go.Scatter(x=df['Date'], y=df['LinearReg_99.9%'], fill='tonexty', mode='lines', line=dict(color='#FF4500', width=0), name='99.9% Quantile', showlegend=False))
        
        
        # Print the number of non-null data points in df['Value']
        num_data_points = df['Value'].count()
        print(f"Number of data points in df['Value']: {num_data_points}")
        
        # Print the total number of data points in df['Value']
        num_data_points = len(df['Value'])
        print(f"Total number of data points in df['Value']: {num_data_points}")
        
        num_data_points_formatted = "{:,.0f}".format(num_data_points)
        
        # Get the latest values for Price and 50% Quantile
        latest_date = df['Date'].iloc[-1]
        latest_price = df['Value'].iloc[-1]
        latest_50_percent = df['LinearReg_50%'].iloc[-1]
        
        latest_01_percent = df['LinearReg_0.1%'].iloc[-1]
        latest_5_percent = df['LinearReg_5%'].iloc[-1]
        latest_90_percent = df['LinearReg_90%'].iloc[-1]
        latest_98_percent = df['LinearReg_98%'].iloc[-1]
        latest_999_percent = df['LinearReg_99.9%'].iloc[-1]
        
        annotations = []
        
        latest_date_formatted = latest_date.strftime("%d %B %Y")
        latest_price_formatted = "{:,.0f}".format(latest_price)
        latest_50_percent_formatted = "{:,.0f}".format(latest_50_percent)
        
        latest_01_percent_formatted = "{:,.0f}".format(latest_01_percent)
        latest_5_percent_formatted = "{:,.0f}".format(latest_5_percent)
        latest_90_percent_formatted = "{:,.0f}".format(latest_90_percent)
        latest_98_percent_formatted = "{:,.0f}".format(latest_98_percent)
        latest_999_percent_formatted = "{:,.0f}".format(latest_999_percent)
        
        # Calculate the percentage change from latest_50_percent
        percent_change = ((latest_price - latest_50_percent) / latest_50_percent) * 100
        percent_change_formatted = "{:,.2f}%".format(percent_change)
        
        
        # Output the percentage change
        print(f"Latest Price: {latest_price:.2f}")
        print(f"Latest 50% Quantile: {latest_50_percent:.2f}")
        print(f"Percentage Change: {percent_change:.2f}%")
        # Output the percentage change
        
        # Determine if percent_change is above or below fair value
        is_above = percent_change >= 0
        change_type = "above" if is_above else "below"
        formatted_percent_change = abs(percent_change)  # Use absolute value for display
        percent_change_formatted = "{:,.2f}%".format(formatted_percent_change)
        
        # Print the result
        print(f"The latest price is {formatted_percent_change:.2f}% {change_type} Fair Value.")
        
        ###################################################################################################################
        
        # Generate quantiles and labels
        quantiles = np.arange(0.5, 1.0, 0.001) if is_above else np.arange(0.001, 0.501, 0.001)
        quantile_labels = [f'{q * 100:.2f}%' for q in quantiles]
        
        # Create dictionaries to store the predictions
        log_reg_predictions = {}
        linear_reg_predictions = {}
        
        # Perform quantile regression for each quantile
        for q, label in zip(quantiles, quantile_labels):
            quant_reg = sm.QuantReg(y, X_with_const)
            quant_reg_fit = quant_reg.fit(q=q, max_iter=10000, tol=1e-6)
        
            log_reg_predictions[f'QuantRegPredict_{("above" if is_above else "below")}_{label}'] = quant_reg_fit.predict(X_with_const)
            linear_reg_predictions[f'LinearReg_{("above" if is_above else "below")}_{label}'] = np.exp(log_reg_predictions[f'QuantRegPredict_{("above" if is_above else "below")}_{label}'])
        
        # Convert dictionaries into DataFrames
        df_log_predictions = pd.DataFrame(log_reg_predictions)
        df_linear_predictions = pd.DataFrame(linear_reg_predictions)
        
        # Concatenate the new columns into the original DataFrame
        df = pd.concat([df, df_log_predictions, df_linear_predictions], axis=1)
        
        # De-fragment the DataFrame by making a copy
        df = df.copy()
        
        # Get the latest date in the dataframe
        latest_date = df['Date'].max()
        df_latest = df[df['Date'] == latest_date]
        
        # Check if the filtered dataframe is not empty
        if df_latest.empty:
            print(f"No data available for the latest date: {latest_date}")
        
        
        # Initialize variables to store the best match and corresponding quantile
        closest_value = None
        corresponding_log_value = None
        best_quantile_label = None
        min_difference = float('inf')  # Start with a very large value for comparison
        
        # Iterate through all quantiles and their labels
        for q, label in zip(quantiles, quantile_labels):
            col_name = f'LinearReg_{("above" if is_above else "below")}_{label}'
            if col_name in df_latest.columns:
                diff = abs(df_latest[col_name] - latest_price)
                if not diff.empty:
                    idx_min_diff = diff.idxmin()
                    if diff[idx_min_diff] < min_difference:
                        min_difference = diff[idx_min_diff]
                        closest_value = df_latest.loc[idx_min_diff, col_name]
                        corresponding_log_value = df_latest.loc[idx_min_diff, f'QuantRegPredict_{("above" if is_above else "below")}_{label}']
                        best_quantile_label = label
        
        if closest_value is not None:
            # Output the closest value, corresponding log value, and quantile label
            print(f"Closest value to latest_price: {closest_value}")
            print(f"Corresponding log-scale value: {corresponding_log_value}")
            print(f"Quantile: {best_quantile_label}")
            print(f"Date of the closest value: {latest_date}")
        else:
            print("No matching data found.")
        ###################################################################################################################
        ###################################################################################################################
        ###################################################################################################################
        #Table with Predictions
        
        
        
        
        ###################################################################################################################
        ###################################################################################################################
        ###################################################################################################################
        
        
        
        
        # Create the annotation text once
        annotations.append(dict(
            xref='paper', yref='paper', x=0.40, y=-0.05,
            xanchor='center', yanchor='top',
            text='Latest Date: ' + str(latest_date_formatted) + ' (' + str(num_data_points_formatted) + ' Data Points)' +
                 '<br>Latest Price: ' + str(latest_price_formatted) + ' (' + str(percent_change_formatted) + f' {change_type} Fair Value); ' + best_quantile_label + ' Quantile' +
                 '<br>Fair Value: $' + str(latest_50_percent_formatted) + ' (50% Quantile)',
            font=dict(family='Arial', size=16, color='rgb(150,150,150)'),
            align='left',  # Explicit alignment option
            showarrow=False
        ))
        
        
        annotations.append(dict(
            xref='paper', yref='paper', x=0.6, y=-0.05,
            xanchor='center', yanchor='top',
            text='98% to 99.9% Quantile ' + '(' + str(latest_98_percent_formatted) + ' - ' + str(latest_999_percent_formatted) + ')'
            '<br>90% to 98% Quantile ' + '(' + str(latest_90_percent_formatted) + ' - ' + str(latest_98_percent_formatted) + ')'
            '<br>0.1% to 5% Quantile ' + '(' + str(latest_01_percent_formatted) + ' - ' + str(latest_5_percent_formatted) + ')',
            font=dict(family='Arial', size=16, color='rgb(150,150,150)'),
            align='left',  # Explicit alignment option
            showarrow=False
        ))
        
        annotations.append(dict(x=1, y=-0.1,
                         text="Chart by: @CarlesMassa",
                         showarrow=False, xref='paper', yref='paper',
                         xanchor='right', yanchor='auto', xshift=0, yshift=0))
        
        # Update layout
        fig.update_layout(
            title='Power Law Probability Channel',
            xaxis_title='Date',
            yaxis_title='Price (USD)',
            yaxis_type='log',
            hovermode='closest',
            annotations=annotations,
            showlegend=True,
            legend_orientation="h",
            template="plotly_dark"
        )
        
        fig.update_yaxes(showgrid=False)
        
        # Show the figure
        fig.show(config={'modeBarButtonsToAdd':['drawline',
                                                'drawopenpath',
                                                'drawcircle',
                                                'drawrect',
                                                'eraseshape',
                                                'v1hovermode',
                                                'togglespikelines',
                                               ]})
        


        ######################################################################################################################################
        ######################################################################################################################################
        ######################################################################################################################################

        # Create Plotly chart
        fig = go.Figure()
        fig.add_trace(
            go.Scatter(
                x=df.index,
                y=df['Value'],
                mode='lines',
                name='BTC/USD',
                line=dict(color='orange', width=2)
            )
        )

        # Anotations
        annotations = []

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
