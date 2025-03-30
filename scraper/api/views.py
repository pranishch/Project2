from rest_framework.decorators import api_view
from rest_framework.response import Response
from rest_framework import status
from data_analysis.models import BrokerTracker, FloorsheetData, StockwiseBroker
from .serializers import BrokerTrackerSerializer, FloorsheetDataSerializeer, StockwiseBrokerSerializer

@api_view(['GET'])
def get_Floorsheet_Data(request):
    """
    Simple GET endpoint to retrieve all FloorsheetData records
    """
    data = FloorsheetData.objects.all()
    serializer = FloorsheetDataSerializeer(data, many=True)
    return Response(serializer.data, status=status.HTTP_200_OK)

@api_view(['GET'])
def get_broker_tracker(request):
    """
    Simple GET endpoint to retrieve all BrokerTracker records
    """
    data = BrokerTracker.objects.all()
    serializer = BrokerTrackerSerializer(data, many=True)
    return Response(serializer.data, status=status.HTTP_200_OK)

@api_view(['GET'])
def get_stockwise_broker(request):
    """
    Simple GET endpoint to retrieve all BrokerTracker records
    """
    data = StockwiseBroker.objects.all()
    serializer = StockwiseBrokerSerializer(data, many=True)
    return Response(serializer.data, status=status.HTTP_200_OK)