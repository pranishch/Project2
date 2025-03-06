from django.shortcuts import render
from .models import StockData, StockTransaction

# Create your views here.

def stock_list(request):
    """Fetch all stock data from the database and render it in the template."""
    stocks = StockData.objects.all()  # Fetch latest data first
    return render(request, 'TP.html', {'stocks': stocks})

def stock_FS(request):
    """Fetch all stock data of floor sheet from the database and render it in the template"""
    stock_transactions = StockTransaction.objects.all()
    return render(request, 'FS.html',  {'stock_transactions': stock_transactions} )
