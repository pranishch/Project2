from django.shortcuts import render
from django.db.models import Sum
from .models import BrokerVolume

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