from rest_framework import serializers
from data_analysis.models import BrokerTracker, FloorsheetData

class FloorsheetDataSerializeer(serializers.ModelSerializer):
    class Meta:
        model = FloorsheetData
        fields = '__all__'


class BrokerTrackerSerializer(serializers.ModelSerializer):
    class Meta:
        model = BrokerTracker
        fields = '__all__'

