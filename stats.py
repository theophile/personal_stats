import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from sklearn.linear_model import LinearRegression

def check_dates(db):
    query = "SELECT DISTINCT date FROM entries ORDER BY date"
    result = db.cursor.execute(query).fetchall()
    dates = [row[0] for row in result]
    print("Available dates in the entries table:", dates)

def track_partner_orgasms(db, start_date, end_date, output_file='partner_orgasms.png'):
    """Track partner orgasms over a specified time period and save as a PNG file."""
    query = '''
    SELECT date, total_org_partner
    FROM entries
    WHERE date BETWEEN ? AND ?
    '''
    result = db.cursor.execute(query, (start_date, end_date)).fetchall()

    # Create a DataFrame with the result
    df = pd.DataFrame(result, columns=['date', 'partner_orgasms'])
    df['date'] = pd.to_datetime(df['date'], format='%Y.%m.%d')

    # Group by date and sum partner orgasms
    daily_orgasms = df.groupby('date', as_index=False)['partner_orgasms'].sum(numeric_only=True)

    if daily_orgasms.empty:
        print("No data available for the specified date range.")
        return

    plt.figure(figsize=(10, 6))
    
    # Plot daily orgasms
    sns.lineplot(x='date', y='partner_orgasms', data=daily_orgasms, marker='o', errorbar=None, label='Daily Orgasms')

    # Calculate the rolling mean for a smoother trend line
    daily_orgasms['trend'] = daily_orgasms['partner_orgasms'].rolling(window=30, min_periods=1).mean()

    # Plot the smoothed trend line
    plt.plot(daily_orgasms['date'], daily_orgasms['trend'], color='red', label='30-Day Moving Average')

    # Set Y-axis to integer scale
    plt.gca().yaxis.get_major_locator().set_params(integer=True)

    plt.title('Partner Orgasms Over Time')
    plt.xlabel('Date')
    plt.ylabel('Total Partner Orgasms')
    plt.ylim(0, daily_orgasms['partner_orgasms'].max() + 1)  # Set Y-axis limit
    plt.xlim(daily_orgasms['date'].min(), daily_orgasms['date'].max())  # Limit X-axis to actual data range
    plt.legend()
    plt.tight_layout()

    # Save the figure as a PNG file
    plt.savefig(output_file)
    plt.close()  # Close the figure to free up memory

def track_partner_orgasms_with_trend(db, start_date, end_date, output_file='partner_orgasms_trend.png'):
    """Track partner orgasms over a specified time period with a weekly trend line and save as a PNG file."""
    query = '''
    SELECT date, total_org_partner
    FROM entries
    WHERE date BETWEEN ? AND ?
    '''
    result = db.cursor.execute(query, (start_date, end_date)).fetchall()

    # Create a DataFrame with the result
    df = pd.DataFrame(result, columns=['date', 'partner_orgasms'])
    df['date'] = pd.to_datetime(df['date'], format='%Y.%m.%d')

    # Group by date and sum partner orgasms
    daily_orgasms = df.groupby('date', as_index=False)['partner_orgasms'].sum(numeric_only=True)

    if daily_orgasms.empty:
        print("No data available for the specified date range.")
        return

    # Create a new column for the week number
    daily_orgasms['week'] = daily_orgasms['date'].dt.isocalendar().week

    # Group by week to calculate weekly sums
    weekly_data = daily_orgasms.groupby(['week'], as_index=False).agg({'partner_orgasms': 'sum'})

    # Fit linear regression for weekly data
    x = np.array(range(len(weekly_data))).reshape(-1, 1)  # Week indices
    y = weekly_data['partner_orgasms'].values
    model = LinearRegression().fit(x, y)
    weekly_data['trend'] = model.predict(x)

    plt.figure(figsize=(10, 6))
    # Plot the daily orgasms
    sns.lineplot(x='date', y='partner_orgasms', data=daily_orgasms, marker='o', label='Daily Orgasms')
    # Plot the weekly trend line
    plt.plot(weekly_data['week'], weekly_data['trend'], color='red', label='Weekly Trend', linewidth=2)

    plt.title('Partner Orgasms Over Time with Weekly Trend')
    plt.xlabel('Date')
    plt.ylabel('Total Partner Orgasms')
    plt.xticks(rotation=45)
    plt.ylim(0, daily_orgasms['partner_orgasms'].max() + 1)  # Set Y-axis limit
    plt.legend()
    plt.tight_layout()

    # Save the figure as a PNG file
    plt.savefig(output_file)
    plt.close()  # Close the figure to free up memory

def track_sex_streaks_bar(db, start_date, end_date):
    # Query to get all unique dates within the specified range
    query = '''
    SELECT date
    FROM entries
    WHERE date BETWEEN ? AND ?
    ORDER BY date
    '''
    df = pd.read_sql_query(query, db.conn, params=(start_date.replace('-', '.'), end_date.replace('-', '.')))

    # Ensure the dates are in proper datetime format
    df['date'] = pd.to_datetime(df['date'].str.replace('.', '-'))

    # Determine the first and last date with actual data
    if df.empty:
        print("No data available within the specified date range.")
        return
    first_date = df['date'].min()
    last_date = df['date'].max()
    
    # Generate a full date range between the first and last date with data
    full_date_range = pd.date_range(start=first_date, end=last_date)
    
    # Determine which dates have entries (sex days) and which don't (no sex days)
    streaks_df = pd.DataFrame(full_date_range, columns=['date'])
    streaks_df['sex_occurred'] = streaks_df['date'].isin(df['date']).astype(int)  # 1 = sex, 0 = no sex
    
    # Initialize variables for tracking streaks
    streaks = []
    current_streak = {'type': None, 'start': None, 'length': 0}
    
    # Iterate through the full date range to identify streaks
    for _, row in streaks_df.iterrows():
        sex_occurred = row['sex_occurred']
        date = row['date']
        
        if current_streak['type'] is None:  # First streak
            current_streak['type'] = sex_occurred
            current_streak['start'] = date
            current_streak['length'] = 1
        elif current_streak['type'] == sex_occurred:  # Continue streak
            current_streak['length'] += 1
        else:  # Streak ends, save the previous streak
            streaks.append(current_streak)
            current_streak = {'type': sex_occurred, 'start': date, 'length': 1}
    
    # Append the final streak
    streaks.append(current_streak)
    
    # Create the plot
    fig, ax = plt.subplots(figsize=(10, 6))
    
    for streak in streaks:
        # Set the position based on the streak type
        if streak['type'] == 1:  # Positive streak (sex occurred)
            y = streak['length']
            color = 'blue'
        else:  # Negative streak (no sex)
            y = -streak['length']
            color = 'red'
        
        # Plot the streak
        ax.bar(streak['start'], y, width=streak['length'], color=color, align='edge')
    
    # Format the X-axis to display periodic dates
    ax.set_xticks(pd.date_range(start=first_date, end=last_date, periods=10))
    ax.set_xlim([first_date, last_date])  # Limit the x-axis to actual data range
    ax.set_ylabel('Streak Length')
    ax.set_title('Sex Streaks Over Time')

    # Save the plot
    plt.savefig("streaks_pos_neg.png")
    plt.show()
