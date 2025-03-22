from django.shortcuts import render
from django.db.models import Sum
from .models import BrokerVolume, BrokerData
from django.http import JsonResponse
import pandas as pd
from .stockwise_broker import load_floorsheet_data, get_stock_names, process_stock_data


def calculate_volumes(request):
    broker_id = request.GET.get('broker_id', None)
    period = request.GET.get('period', 'latest_date')  # Default to latest date

    # Fetch volumes from the database filtered by time_period
    volumes = BrokerVolume.objects.filter(time_period=period)
    if broker_id:
        volumes = volumes.filter(broker_id=broker_id)

    # Aggregate volumes by broker_id and order by total_volume in descending order
    volumes = volumes.values('broker_id').annotate(total_volume=Sum('total_volume')).order_by('-total_volume')

    context = {
        'volumes': volumes,
        'period': period,
        'broker_id': broker_id,
    }

    return render(request, 'broker_tracker.html', context)


def index(request):
    csv_path = r"C:\Users\Arjun\Desktop\project2\scraper\analysis\floorsheet_floorsheetdata.csv"
    df = load_floorsheet_data(csv_path)
    stock_names, stock_col = get_stock_names(df)
    return render(request, 'stockwise_broker.html', {'stock_names': stock_names})

def get_broker_data(request):
    stock_name = request.GET.get('stock_name')
    csv_path = r"C:\Users\Arjun\Desktop\project2\scraper\analysis\floorsheet_floorsheetdata.csv"
    df = load_floorsheet_data(csv_path)
    _, stock_col = get_stock_names(df)
    result_df = process_stock_data(df, stock_name, stock_col)

    if result_df is not None:
        # Convert DataFrame to a list of dictionaries for the template
        broker_data = result_df.to_dict('records')
        
        # Pass the data to the template
        return render(request, 'broker_table.html', {'broker_data': broker_data, 'stock_name': stock_name})
    else:
        return render(request, 'broker_table.html', {'error': 'No data available for the selected stock.'})