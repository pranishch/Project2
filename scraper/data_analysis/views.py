from django.shortcuts import render
from django.db.models import Sum
from .models import BrokerTracker, StockwiseBroker, FloorsheetData
from django.http import JsonResponse
from data_analysis.stockwise_broker import get_stock_names
from datetime import datetime, timedelta

def calculate_volumes(request):
    broker_id = request.GET.get('broker_id', None)
    period = request.GET.get('period', 'latest_date')  # Default to latest date

    # Fetch volumes from the database filtered by time_period
    volumes = BrokerTracker.objects.filter(time_period=period)
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
    """
    Render the main page with a dropdown of stock names.
    """
    stock_names = get_stock_names()
    return render(request, 'stockwise_broker.html', {'stock_names': stock_names})

def get_broker_data(request):
    """
    Fetch and render broker data for the selected stock and time frame.
    """
    stock_name = request.GET.get('stock_name')  # Get the selected stock from the dropdown
    time_frame = request.GET.get('time_frame', 'daily')  # Get the selected time frame

    if stock_name:
        # Process and save data for the selected stock and time frame
        from ..data_analysis.stockwise_broker import save_broker_data
        save_broker_data(stock_name, time_frame)

        # Fetch the saved data from the database
        broker_data = StockwiseBroker.objects.filter(stock_name=stock_name, time_frame=time_frame)
        grand_total_volume = broker_data.aggregate(Sum('volume'))['volume__sum']

        return render(request, 'broker_table.html', {
            'broker_data': broker_data,
            'grand_total_volume': grand_total_volume,
            'time_frame': time_frame
        })
    else:
        return render(request, 'broker_table.html', {'error': 'No stock selected.'})