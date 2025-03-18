import dash
from dash import dcc, html, Input, Output
import plotly.graph_objects as go
import pandas as pd
import dash_bootstrap_components as dbc
import plotly.express as px
from load_to_db import read_json_file
from sqlalchemy import create_engine


# conn = psycopg2.connect(
#     dbname="postgres",
#     user="postgres",
#     password="mysecretpassword",
#     host="localhost",
#     port="5433"
# )
df = None

db_par = read_json_file("config.json")["postgres"]
connection_string = 'postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}'.format(**db_par)
engine = create_engine(connection_string)

# Define the SQL query to retrieve the data
sql_query = """
SELECT posted_speed_limit, device_condition, lighting_condition, street_direction, most_severe_injury, airbag_deployed,
injury_classification, sex, vehicle_type, first_contact_point, vehicle_defect, vehicle_year, crash_month,
injuries_total, weather_condition, crash_type, road_defect, crash_hour, crash_day_of_week FROM crash_vehical_people
"""
# Use Pandas to read SQL query results into a DataFrame
df = pd.read_sql(sql_query, engine)
# df = df.head(50000)
# mongo_para = read_json_file("config.json")["mongo"]
# client = MongoClient(mongo_para.get("host"), mongo_para.get("port"))
# db = client["test-database2"]
# collection = db["crash"] 
# columns = ["posted_speed_limit", "device_condition", "lighting_condition", "street_direction", "injuries_total",
#            "street_direction", "injuries_total", "weather_condition", "crash_type","road_defect","crash_hour","crash_day_of_week"]

# query = {}
# projection = {"_id": 0}

# for x in columns:
#     projection[x] = 1

# documents = collection.find(query, projection)

# df1 = pd.DataFrame(list(documents))
# df1 = df1.head(5000)

# print(df.head())
# print(df.columns)
# df['posted_speed_limit'] = pd.to_numeric(df['posted_speed_limit'], errors='coerce')
# df['injuries_total'] = pd.to_numeric(df['injuries_total'], errors='coerce')
# df['DEVICE_CONDITION'] = df['DEVICE_CONDITION'].astype('category')
# df['lighting_condition'] = df['lighting_condition'].astype('category')
# df['device_condition'] = df["device_condition"].fillna("UNKNOWN")
# df['lighting_condition'] = df["lighting_condition"].fillna("UNKNOWN")


app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
row = html.Div(
    [
        # dbc.Row(
        #     [
        #         dbc.Col(html.Div("Device Condition")),
        #         dbc.Col(html.Div("Lighting Condition")),
        #     ]
        # ),
    ]
)

app.layout = dbc.Container([
    html.Div([
        html.H1('Dashboard', style={'textAlign': 'center', 'marginBottom': '20px', 'color': 'white'})
    ], className='mb-4', style={'background-color': '#007bff', 'padding': '20px'}),
    row,
    dbc.Row([
        dbc.Col([
            dcc.Dropdown(
                id='device-dropdown',
                options=[{'label': str(device), 'value': device} for device in df['device_condition'].unique()],
                multi=True,
                value=None,
                placeholder='Select Device Condition'
            )
        ], width=6),
        dbc.Col([
            dcc.Dropdown(
                id='light-dropdown',
                options=[{'label': light, 'value': light} for light in df['lighting_condition'].unique()],
                multi=True,
                value=None,
                placeholder='Select Lighting Condition'
            )
        ], width=6)
    ], className='mb-4', style={'display': 'none'}), # hiding the filters

    dbc.Row([
        dbc.Col([
            dcc.Graph(id='pie-chart')
        ], width=4),
        dbc.Col([
            dcc.Graph(id='bar-chart')
        ], width=4),
        dbc.Col([
            dcc.Graph(id='bar-chart2')
        ], width=4)
    ]),

    dbc.Row([
        dbc.Col([
            dcc.Graph(id='bar-chart-crash_hour')
        ], width=4),
        dbc.Col([
            dcc.Graph(id='bar-chart-mso')
        ], width=4),
        dbc.Col([
            dcc.Graph(id='bar-chart-road-defect')
        ], width=4)
    ]),
    
    dbc.Row([
        dbc.Col([
            dcc.Graph(id='heat-map-lightvsweek')
        ], width=6),
        dbc.Col([
            dcc.Graph(id='heat-map-lightvsmonth')
        ], width=6)
    ]),
    dbc.Row([
        dbc.Col([
            dcc.Graph(id='bar-chart_vehicle_year')
        ], width=6),
        dbc.Col([
            dcc.Graph(id='bar-chart_vehicle_defect')
        ], width=6)
    ]),
        dbc.Row([
        dbc.Col([
            dcc.Graph(id='bar-chart_first_contact_point')
        ], width=6),
        dbc.Col([
            dcc.Graph(id='bar-chart_vehicle_type')
        ], width=6)
    ]),
    dbc.Row([
        dbc.Col([
            dcc.Graph(id='bar-gender')
        ], width=6),
  
    ])
], fluid=True)

@app.callback(
    Output('bar-gender', 'figure'),
    [Input('device-dropdown', 'value'),
     Input('light-dropdown', 'value')]
)
def update_bar_chart(age_filter, gender_filter):
    filtered_df = apply_filter(df, age_filter, gender_filter)
    category_counts = filtered_df['sex'].value_counts()
    fig = go.Figure(data=go.Bar(x=category_counts.index, y=category_counts.values))
    fig.update_layout(title='Gender', xaxis_title='Gender', yaxis_title='Count')
    return fig

@app.callback(
    Output('bar-chart_vehicle_type', 'figure'),
    [Input('device-dropdown', 'value'),
     Input('light-dropdown', 'value')]
)
def update_bar_chart(age_filter, gender_filter):
    filtered_df = apply_filter(df, age_filter, gender_filter)
    filtered_df = filtered_df[filtered_df['vehicle_type'].isin(['UNKNOWN/NA', 'NONE', 'OTHER']) == False]
    category_counts = filtered_df['vehicle_type'].value_counts()
    fig = go.Figure(data=go.Bar(x=category_counts.index, y=category_counts.values))
    fig.update_layout(title='Most Crashed Vehicle Types', xaxis_title='Vehicle Type', yaxis_title='Crash Count')
    return fig

@app.callback(
    Output('bar-chart_first_contact_point', 'figure'),
    [Input('device-dropdown', 'value'),
     Input('light-dropdown', 'value')]
)
def update_bar_chart(age_filter, gender_filter):
    filtered_df = apply_filter(df, age_filter, gender_filter)
    #filtered_df = filtered_df.loc[filtered_df["injury_classification"] == "FATAL"]
    filtered_df = filtered_df[filtered_df['first_contact_point'].isin(['UNKNOWN']) == False]
    category_counts = filtered_df['first_contact_point'].value_counts()
    fig = go.Figure(data=go.Bar(x=category_counts.index, y=category_counts.values))
    fig.update_layout(title='Distribution of First Point of Contact', xaxis_title='First Contact Point', yaxis_title='Crash Count')
    return fig


@app.callback(
    Output('bar-chart_vehicle_defect', 'figure'),
    [Input('device-dropdown', 'value'),
     Input('light-dropdown', 'value')]
)
def update_bar_chart(age_filter, gender_filter):
    filtered_df = apply_filter(df, age_filter, gender_filter)
    filtered_df = filtered_df[filtered_df['vehicle_defect'].isin(['UNKNOWN', 'NONE', 'OTHER']) == False]
    category_counts = filtered_df['vehicle_defect'].value_counts()
    fig = go.Figure(data=go.Bar(x=category_counts.index, y=category_counts.values))
    fig.update_layout(title='Distribution of Vehicle Defect', xaxis_title='Vehicle Defect', yaxis_title='Crash Count')
    return fig

@app.callback(
    Output('bar-chart_vehicle_year', 'figure'),
    [Input('device-dropdown', 'value'),
     Input('light-dropdown', 'value')]
)
def update_bar_chart(age_filter, gender_filter):
    filtered_df = apply_filter(df, age_filter, gender_filter)
    category_counts = filtered_df['vehicle_year'].value_counts()
    fig = go.Figure(data=go.Bar(x=category_counts.index, y=category_counts.values))
    fig.update_layout(title='Distribution of Vehicle Manufacturing Years', xaxis_title='Vehicle Year', yaxis_title='Crash Count')
    return fig


@app.callback(
    Output('heat-map-lightvsweek', 'figure'),
    [Input('device-dropdown', 'value'),
     Input('light-dropdown', 'value')]
)
def update_bar_chart(age_filter, gender_filter):
    filtered_df = apply_filter(df, age_filter, gender_filter)
    weekly_counts = df.groupby(['lighting_condition', 'crash_day_of_week']).size().unstack(fill_value=0)
    fig = go.Figure(data=go.Heatmap(z=weekly_counts, x=weekly_counts.columns, y=weekly_counts.index, colorscale='RdBu', zmin=weekly_counts.values.min(), zmax=weekly_counts.values.max()))
    fig.update_layout(title='Number of Crashes by Lighting Condition and Day of Week', xaxis_title='Day of Week', yaxis_title='Lighting Condition')
    return fig

@app.callback(
    Output('heat-map-lightvsmonth', 'figure'),
    [Input('device-dropdown', 'value'),
     Input('light-dropdown', 'value')]
)
def update_bar_chart(age_filter, gender_filter):
    filtered_df = apply_filter(df, age_filter, gender_filter)
    weekly_counts = df.groupby(['lighting_condition', 'crash_month']).size().unstack(fill_value=0)
    fig = go.Figure(data=go.Heatmap(z=weekly_counts, x=weekly_counts.columns, y=weekly_counts.index, colorscale='RdBu', zmin=weekly_counts.values.min(), zmax=weekly_counts.values.max()))
    fig.update_layout(title='Number of Crashes by Lighting Condition and Month', xaxis_title='Month', yaxis_title='Lighting Condition')
    return fig


@app.callback(
    Output('bar-chart-road-defect', 'figure'),
    [Input('device-dropdown', 'value'),
     Input('light-dropdown', 'value')]
)
def update_bar_chart(age_filter, gender_filter):
    filtered_df = apply_filter(df, age_filter, gender_filter)
    avg_age = filtered_df.groupby('road_defect')['injuries_total'].sum().reset_index()
    fig = go.Figure(data=go.Bar(x=avg_age['road_defect'], y=avg_age['injuries_total']))
    fig.update_layout(title='Road defect vs Total Injuries', xaxis_title='Road defect', yaxis_title='Total Injury')
    return fig

@app.callback(
    Output('bar-chart-mso', 'figure'),
    [Input('device-dropdown', 'value'),
     Input('light-dropdown', 'value')]
)
def update_bar_chart(age_filter, gender_filter):
    filtered_df = apply_filter(df, age_filter, gender_filter)
    avg_age = filtered_df.groupby('most_severe_injury')['injuries_total'].sum().reset_index()
    fig = go.Figure(data=go.Bar(x=avg_age['most_severe_injury'], y=avg_age['injuries_total']))
    fig.update_layout(title='Severity of Injury by Total Injury', xaxis_title='Severity of Injury', yaxis_title='Total Injury')
    return fig


@app.callback(
    Output('bar-chart-crash_hour', 'figure'),
    [Input('device-dropdown', 'value'),
     Input('light-dropdown', 'value')]
)
def update_bar_chart(age_filter, gender_filter):
    filtered_df = apply_filter(df, age_filter, gender_filter)
    avg_age = filtered_df.groupby('crash_hour')['injuries_total'].sum().reset_index()
    # fig = go.Figure(data=go.Scatter(x=avg_age['street_direction'], y=avg_age['injuries_total'], mode='lines+markers'))
    fig = go.Figure(data=go.Scatter(x=avg_age['crash_hour'], y=avg_age['injuries_total'], mode='lines+markers'))
    fig.update_layout(title='Analyzing Crash Injuries at various hours of a Day', xaxis_title='Crash hour', yaxis_title='Total Injury')
    return fig


@app.callback(
    Output('pie-chart', 'figure'),
    [Input('device-dropdown', 'value'),
     Input('light-dropdown', 'value')]
)
def update_pie_chart(device_filter, light_filter):
    filtered_df = apply_filter(df, device_filter, light_filter)
    avg_age = filtered_df.groupby('lighting_condition')['injuries_total'].sum().reset_index()
    fig = go.Figure(data=[go.Pie(labels=filtered_df['lighting_condition'], values=filtered_df['injuries_total'])])
    fig.update_layout(title='Lighting Condition')
    return fig



@app.callback(
    Output('bar-chart', 'figure'),
    [Input('device-dropdown', 'value'),
     Input('light-dropdown', 'value')]
)
def update_bar_chart(device_filter, light_filter):
    filtered_df = apply_filter(df, device_filter, light_filter)
    avg_age = filtered_df.groupby('weather_condition')['injuries_total'].sum().reset_index()
    fig = go.Figure(data=go.Scatter(x=avg_age['weather_condition'], y=avg_age['injuries_total'], mode='lines+markers'))
    fig.update_layout(title='Weather Condition by Total Injury', xaxis_title='Weather Condition', yaxis_title='Total Injury')
    return fig


@app.callback(
    Output('bar-chart2', 'figure'),
    [Input('device-dropdown', 'value'),
     Input('light-dropdown', 'value')]
)
def update_bar_chart(age_filter, gender_filter):
    filtered_df = apply_filter(df, age_filter, gender_filter)
    avg_age = filtered_df.groupby('street_direction')['injuries_total'].sum().reset_index()
    fig = go.Figure(data=go.Bar(x=avg_age['street_direction'], y=avg_age['injuries_total']))
    fig.update_layout(title='Street Direction by Total Injury', xaxis_title='Direction', yaxis_title='Total Injury')
    return fig





def apply_filter(filtered_df, age_filter, gender_filter):
    if age_filter is not None and len(age_filter) > 0:
        filtered_df = filtered_df[filtered_df['device_condition'].isin(age_filter) ]
    if gender_filter is not None and len(gender_filter) > 0:
        filtered_df = filtered_df[filtered_df['lighting_condition'].isin(gender_filter) ]
    return filtered_df

# # Run the app
if __name__ == '__main__':
    try:
        app.run_server(host='0.0.0.0', debug=True)
    except:
        print("")

def run_app(done):
    app.run_server(host='0.0.0.0', debug=True)
    print("Demo")

