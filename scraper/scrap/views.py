from django.shortcuts import render
import csv
import pandas as pd
import os
# Create your views here.
from .models import StockData

def stock_list(request):
    """Fetch all stock data from the  database and render it in the template."""
    stocks = StockData.objects.all()  # Fetch latest data first
    # stocks = StockData.objects.all().order_by('serial_number')  # Ensure ascending order
    return render(request, 'TP.html', {'stocks': stocks})