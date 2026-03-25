from django.shortcuts import render
from django.http import HttpResponse
import random
import pandas as pd
import xlrd
import xlwt
import re
from xlwt.Workbook import *
from pandas import ExcelWriter
import xlsxwriter
from plotly.offline import plot
#import plotly.graph_objs as go
import plotly.express as go
import plotly.graph_objects as go_2
from plotly.subplots import make_subplots
from django.shortcuts import render
from plotly.offline import plot
from plotly.graph_objs import Scatter
from datetime import datetime
import numpy
import json
from collections import OrderedDict
from django.db import connection
from pandas import DataFrame
# from django.db import transaction
import threading
from concurrent.futures import ThreadPoolExecutor
import numpy as np
import traceback

# Global filter: Only these Market Units should be shown and processed
ALLOWED_MARKET_UNITS = ['UK&I','Southern Europe & Middle East','Northern Europe','DACH','EMEA Others','CE Others']

# Create your views here.


def parse_selected_values(request, vertical, region, optType, tier, index):
    # Filter selected regions to only include allowed Market Units
    selected_regions = request.GET.getlist("unq_region[]")
    if selected_regions:
        selected_regions = [r for r in selected_regions if r in ALLOWED_MARKET_UNITS]
        if not selected_regions:
            selected_regions = ALLOWED_MARKET_UNITS
    else:
        selected_regions = [r for r in region if r in ALLOWED_MARKET_UNITS] if region else ALLOWED_MARKET_UNITS
        
    selected_values = {
        "selected_vertical": ','.join(request.GET.getlist("vertical[]")) or ','.join(vertical),
        "selected_region": ','.join(selected_regions),
        
        # --- UPDATE THIS LINE (Default to 5 Active Stages) ---
        "selected_salesstage": ','.join(request.GET.getlist("sales_stage[]")) or ','.join(['1. Engagement', '2. Shaping', '3. Solutioning', '4. End-Game', '5. Negotiation']),
        
        "selected_dealsize": ','.join(request.GET.getlist("deal_size[]")) or ','.join(["$0m - $2.5m", "$2.5m - $10m", "$10m - $25m", "$25m - $50m", ">= $50m"]),
        "selected_oppType": ','.join(request.GET.getlist("type[]")) or ','.join(optType),
        "selected_bu": ','.join(request.GET.getlist("unique_mcu[]")) or "All",
        "selected_sbu": ','.join(request.GET.getlist("unique_sbu[]")) or "All",
        "selected_account": ','.join(request.GET.getlist("unique_account_name[]")) or "All",
        "selected_horizontal": ','.join(request.GET.getlist("unique_horizontal[]")) or "All",
        "selected_tier": ','.join(request.GET.getlist("tier_data[]")) or ','.join(tier),
        "idx_label_selected" : request.GET.get("idx_label") or index,
        "selected_winzoneID": request.GET.get("winzone_id") or "",
        "selected_opp_source": ','.join(request.GET.getlist("opp_source[]")) or "All",
        "selected_acc_type": ','.join(request.GET.getlist("acc_type[]")) or "All",
    }

    return selected_values
def parse_selected_valueswinreport(request,vertical,region,optType,tier,index):
    # print(request.GET.get("winzone_id"))
    # print(request.GET.getlist("unq_region[]"))
    # print(region,vertical)
     # Filter selected regions to only include allowed Market Units
    selected_regions = request.GET.getlist("unq_region[]")
    if selected_regions:
        # Filter out any Market Units that are not in the allowed list
        selected_regions = [r for r in selected_regions if r in ALLOWED_MARKET_UNITS]
        # If nothing left after filtering, use all allowed units
        if not selected_regions:
            selected_regions = ALLOWED_MARKET_UNITS
    else:
        # Default to allowed regions if none selected
        selected_regions = [r for r in region if r in ALLOWED_MARKET_UNITS] if region else ALLOWED_MARKET_UNITS
    
    stages = request.GET.getlist("sales_stage[]")
    cleaned = [s for s in stages if s != "Duplicate"]
    selected_values = {
        "selected_vertical": ','.join(request.GET.getlist("vertical[]")) or ','.join(vertical),
        # "selected_region": ','.join(request.GET.getlist("unq_region[]")) or ','.join(region),
        "selected_region": ','.join(selected_regions),
        "selected_salesstage": ','.join(cleaned) or ','.join(['1. Engagement','2. Shaping','Client Withdraw','Cognizant Withdraw','3. Solutioning','4. End-Game','5. Negotiation',"Won","Lost"]),
        "selected_dealsize": ','.join(request.GET.getlist("deal_size[]")) or ','.join(["$0m - $2.5m", "$2.5m - $10m", "$10m - $25m", "$25m - $50m", ">= $50m"]),
        "selected_oppType": ','.join(request.GET.getlist("type[]")) or ','.join(optType),
        "selected_bu": ','.join(request.GET.getlist("unique_mcu[]")) or "All",
        "selected_sbu": ','.join(request.GET.getlist("unique_sbu[]")) or "All",
        "selected_account": ','.join(request.GET.getlist("unique_account_name[]")) or "All",
        "selected_horizontal": ','.join(request.GET.getlist("unique_horizontal[]")) or "All",
        "selected_tier": ','.join(request.GET.getlist("tier_data[]")) or ','.join(tier),
        "idx_label_selected" : request.GET.get("idx_label") or index,
        "selected_winzoneID": request.GET.get("winzone_id") or "",
        "selected_opp_source": ','.join(request.GET.getlist("opp_source[]")) or "All",
        "selected_acc_type": ','.join(request.GET.getlist("acc_type[]")) or "All",
    }

    return selected_values

from django.http import JsonResponse

def AjaxCallForWonLostDeals(request):
    """AJAX endpoint to fetch Won/Lost deals with configurable row limit and optional date range filter"""
    try:
        row_limit_raw = request.GET.get('row_limit', '20')
        if row_limit_raw.lower() == 'all':
            row_limit = 'all'
        else:
            row_limit = int(row_limit_raw)
            if row_limit not in [10, 20, 30, 50, 100, 300]:
                row_limit = 20
        
        idx_label = request.GET.get('idx_label', '')
        vertical = request.GET.get('vertical', '')
        region = request.GET.get('region', '')
        dealsize = request.GET.get('dealsize', '')
        opptype = request.GET.get('opptype', '')
        account_name = request.GET.get('account', '')
        bu = request.GET.get('bu', '')
        sbu = request.GET.get('sbu', '')
        
        # New date range filter parameters
        use_date_filter = request.GET.get('use_date_filter', 'false') == 'true'
        start_date = request.GET.get('start_date', '')
        end_date = request.GET.get('end_date', '')
        
        with connection.cursor() as cursor:
            vertical_filter = "','".join(vertical.split(',')) if vertical else ''
            region_filter = "','".join(region.split(',')) if region else ''
            dealsize_filter = "','".join(dealsize.split(',')) if dealsize else ''
            opptype_filter = "','".join(opptype.split(',')) if opptype else ''
            
            # Build BU and SBU filter conditions
            bu_values = [v.strip() for v in bu.split(',') if v.strip()] if bu else []
            if bu_values and 'All' not in bu_values:
                bu_filter_sql = "AND [BU] IN ('" + "','".join(bu_values) + "')"
            else:
                bu_filter_sql = ''
            
            sbu_values = [v.strip() for v in sbu.split(',') if v.strip()] if sbu else []
            if sbu_values and 'All' not in sbu_values:
                sbu_filter_sql = "AND [SBU1] IN ('" + "','".join(sbu_values) + "')"
            else:
                sbu_filter_sql = ''
            
            # Build date condition based on filter mode
            if use_date_filter and start_date and end_date:
                # Use custom date range filter - cast to DATE to ignore time component
                date_condition_wonlost = f"AND CAST([Last Modified Date] AS DATE) >= CONVERT(DATE, '{start_date}', 101) AND CAST([Last Modified Date] AS DATE) <= CONVERT(DATE, '{end_date}', 101)"
                # Custom date range may extend beyond the selected idx_label snapshot,
                # so always use the LATEST snapshot. Use a SQL subquery to avoid
                # Python-side type/format conversion issues with the uploadedon column.
                uploadedon_condition = "uploadedon = (SELECT MAX(uploadedon) FROM [dbo].[Pipelinedata])"
                print(f"[WonLost DateFilter] start={start_date}, end={end_date}, snapshot=MAX(uploadedon) subquery")
            else:
                # Use traditional idx_label-based filtering (last 2 weeks from snapshot)
                ref_date = idx_label
                if ref_date:
                    date_condition_wonlost = f"AND [Last Modified Date] >= DATEADD(WEEK, -2, CONVERT(DATETIME, '{ref_date}', 101))"
                else:
                    date_condition_wonlost = f"AND [Last Modified Date] >= DATEADD(WEEK, -2, GETDATE())"
                uploadedon_condition = f"uploadedon = '{idx_label}'"

            top_clause = '' if row_limit == 'all' else f'TOP {row_limit}'
            # Apply all filters including vertical, region, dealsize, opptype even when date filter is active
            if use_date_filter and start_date and end_date:
                wonlost_query = f"""
                    SELECT {top_clause} 
                        [WinZone Opportunity ID],
                        [Opportunity Name],
                        [Account Name],
                        [R_Vertical] as Vertical,
                        [Market Unit],
                        [Sales Stage],
                        CASE WHEN [Qualified] = 1 THEN 'Qualified' ELSE 'Not Qualified' END as [Qualified],
                        [Last Modified Date],
                        [CloseDate] as [Close Date],
                        ROUND(ISNULL([Net TCV], 0)/1000000, 6) as [Net TCV]
                    FROM [dbo].[Pipelinedata]
                    WHERE {uploadedon_condition}
                        AND [R_Vertical] IN ('{vertical_filter}')
                        AND [Market Unit] IN ('{region_filter}')
                        AND [Sales Stage] IN ('Won', 'Lost')
                        AND [DealSize] IN ('{dealsize_filter}')
                        AND [R_Deal Type] IN ('{opptype_filter}')
                        {"AND [Account Name] IN ('" + "','".join(account_name.split(',')) + "')" if account_name and 'All' not in account_name.split(',') else ''}
                        {bu_filter_sql}
                        {sbu_filter_sql}
                        {date_condition_wonlost}
                    ORDER BY [Net TCV] DESC
                """
            else:
                wonlost_query = f"""
                    SELECT {top_clause} 
                        [WinZone Opportunity ID],
                        [Opportunity Name],
                        [Account Name],
                        [R_Vertical] as Vertical,
                        [Market Unit],
                        [Sales Stage],
                        CASE WHEN [Qualified] = 1 THEN 'Qualified' ELSE 'Not Qualified' END as [Qualified],
                        [Last Modified Date],
                        [CloseDate] as [Close Date],
                        ROUND(ISNULL([Net TCV], 0)/1000000, 6) as [Net TCV]
                    FROM [dbo].[Pipelinedata]
                    WHERE {uploadedon_condition}
                        AND [R_Vertical] IN ('{vertical_filter}')
                        AND [Market Unit] IN ('{region_filter}')
                        AND [Sales Stage] IN ('Won', 'Lost')
                        AND [DealSize] IN ('{dealsize_filter}')
                        AND [R_Deal Type] IN ('{opptype_filter}')
                        {"AND [Account Name] IN ('" + "','".join(account_name.split(',')) + "')" if account_name and 'All' not in account_name.split(',') else ''}
                        {bu_filter_sql}
                        {sbu_filter_sql}
                        {date_condition_wonlost}
                    ORDER BY [Net TCV] DESC
                """
            print(f"[WonLost] Executing query with uploadedon_condition={uploadedon_condition}, use_date_filter={use_date_filter}")
            cursor.execute(wonlost_query)
            col_names = [col_desc[0] for col_desc in cursor.description]
            wonLostDf = DataFrame.from_records(cursor.fetchall(), columns=col_names)
            print(f"[WonLost] Query returned {len(wonLostDf)} rows")
            
            if not wonLostDf.empty:
                if 'Last Modified Date' in wonLostDf.columns:
                    wonLostDf['Last Modified Date'] = pd.to_datetime(wonLostDf['Last Modified Date'], errors='coerce').dt.strftime('%m/%d/%Y')
                if 'Close Date' in wonLostDf.columns:
                    wonLostDf['Close Date'] = pd.to_datetime(wonLostDf['Close Date'], errors='coerce').dt.strftime('%m/%d/%Y')
            
            wonLostDealsData = wonLostDf.to_dict(orient="records")
        
        return JsonResponse({'success': True, 'data': wonLostDealsData})
    except Exception as e:
        print(f"Error in AjaxCallForWonLostDeals: {e}")
        traceback.print_exc()
        return JsonResponse({'success': False, 'error': str(e), 'data': []})

def AjaxCallForTopDeals(request):
    """AJAX endpoint to fetch Top N deals with configurable row limit"""
    try:
        row_limit_raw = request.GET.get('row_limit', '20')
        if row_limit_raw.lower() == 'all':
            row_limit = 'all'
        else:
            row_limit = int(row_limit_raw)
            if row_limit not in [10, 20, 30, 50, 100, 300]:
                row_limit = 20
        
        idx_label = request.GET.get('idx_label', '')
        vertical = request.GET.get('vertical', '')
        region = request.GET.get('region', '')
        dealsize = request.GET.get('dealsize', '')
        opptype = request.GET.get('opptype', '')
        salesstage = request.GET.get('salesstage', '')
        account_name = request.GET.get('account', '')
        bu = request.GET.get('bu', '')
        sbu = request.GET.get('sbu', '')
        
        with connection.cursor() as cursor:
            vertical_filter = "','".join(vertical.split(',')) if vertical else ''
            region_filter = "','".join(region.split(',')) if region else ''
            dealsize_filter = "','".join(dealsize.split(',')) if dealsize else ''
            opptype_filter = "','".join(opptype.split(',')) if opptype else ''
            salesstage_filter = "','".join(salesstage.split(',')) if salesstage else ''
            
            # Build BU and SBU filter conditions
            bu_values = [v.strip() for v in bu.split(',') if v.strip()] if bu else []
            if bu_values and 'All' not in bu_values:
                bu_filter_sql = "AND [BU] IN ('" + "','".join(bu_values) + "')"
            else:
                bu_filter_sql = ''
            
            sbu_values = [v.strip() for v in sbu.split(',') if v.strip()] if sbu else []
            if sbu_values and 'All' not in sbu_values:
                sbu_filter_sql = "AND [SBU1] IN ('" + "','".join(sbu_values) + "')"
            else:
                sbu_filter_sql = ''
            
            top_clause = '' if row_limit == 'all' else f'TOP {row_limit}'
            top_query = f"""
                SELECT {top_clause} 
                    [WinZone Opportunity ID],
                    [Opportunity Name],
                    [Account Name],
                    [R_Vertical] as Vertical,
                    [Market Unit],
                    [Sales Stage],
                    CASE WHEN [Qualified] = 1 THEN 'Qualified' ELSE 'Not Qualified' END as [Qualified],
                    [R_Deal Type],
                    [CloseDate] as [Close Date],
                    ROUND(ISNULL([Net TCV], 0)/1000000, 6) as [Net TCV]
                FROM [dbo].[Pipelinedata]
                WHERE uploadedon = '{idx_label}'
                    AND [R_Vertical] IN ('{vertical_filter}')
                    AND [Market Unit] IN ('{region_filter}')
                    AND [Sales Stage] IN ('{salesstage_filter}')
                    AND [DealSize] IN ('{dealsize_filter}')
                    AND [R_Deal Type] IN ('{opptype_filter}')
                    {"AND [Account Name] IN ('" + "','".join(account_name.split(',')) + "')" if account_name and 'All' not in account_name.split(',') else ''}
                    {bu_filter_sql}
                    {sbu_filter_sql}
                ORDER BY [Net TCV] DESC
            """
            cursor.execute(top_query)
            col_names = [col_desc[0] for col_desc in cursor.description]
            topDf = DataFrame.from_records(cursor.fetchall(), columns=col_names)
            if not topDf.empty and 'Close Date' in topDf.columns:
                topDf['Close Date'] = pd.to_datetime(topDf['Close Date'], errors='coerce').dt.strftime('%m/%d/%Y')
            topDealsData = topDf.to_dict(orient="records")
        
        return JsonResponse({'success': True, 'data': topDealsData})
    except Exception as e:
        print(f"Error in AjaxCallForTopDeals: {e}")
        traceback.print_exc()
        return JsonResponse({'success': False, 'error': str(e), 'data': []})

def AjaxCallForRecentCreatedDeals(request):
    """AJAX endpoint to fetch Recent Created Deals with configurable row limit and optional date range filter"""
    try:
        row_limit_raw = request.GET.get('row_limit', '20')
        if row_limit_raw.lower() == 'all':
            row_limit = 'all'
        else:
            row_limit = int(row_limit_raw)
            if row_limit not in [10, 20, 30, 50, 100, 300]:
                row_limit = 20
        
        idx_label = request.GET.get('idx_label', '')
        vertical = request.GET.get('vertical', '')
        region = request.GET.get('region', '')
        dealsize = request.GET.get('dealsize', '')
        opptype = request.GET.get('opptype', '')
        account_name = request.GET.get('account', '')
        bu = request.GET.get('bu', '')
        sbu = request.GET.get('sbu', '')
        
        # New date range filter parameters
        use_date_filter = request.GET.get('use_date_filter', 'false') == 'true'
        start_date = request.GET.get('start_date', '')
        end_date = request.GET.get('end_date', '')
        
        with connection.cursor() as cursor:
            vertical_filter = "','".join(vertical.split(',')) if vertical else ''
            region_filter = "','".join(region.split(',')) if region else ''
            dealsize_filter = "','".join(dealsize.split(',')) if dealsize else ''
            opptype_filter = "','".join(opptype.split(',')) if opptype else ''
            
            # Build BU and SBU filter conditions
            bu_values = [v.strip() for v in bu.split(',') if v.strip()] if bu else []
            if bu_values and 'All' not in bu_values:
                bu_filter_sql = "AND [BU] IN ('" + "','".join(bu_values) + "')"
            else:
                bu_filter_sql = ''
            
            sbu_values = [v.strip() for v in sbu.split(',') if v.strip()] if sbu else []
            if sbu_values and 'All' not in sbu_values:
                sbu_filter_sql = "AND [SBU1] IN ('" + "','".join(sbu_values) + "')"
            else:
                sbu_filter_sql = ''
            
            # Build date condition based on filter mode
            if use_date_filter and start_date and end_date:
                # Use custom date range filter
                # Use DATE cast to ignore time components for proper date-only comparison
                date_condition = f"AND CAST([Created Date] AS DATE) >= CONVERT(DATE, '{start_date}', 101) AND CAST([Created Date] AS DATE) <= CONVERT(DATE, '{end_date}', 101)"
                # Custom date range may extend beyond the selected idx_label snapshot,
                # so always use the LATEST snapshot. Use a SQL subquery to avoid
                # Python-side type/format conversion issues with the uploadedon column.
                uploadedon_condition = "uploadedon = (SELECT MAX(uploadedon) FROM [dbo].[Pipelinedata])"
                print(f"[RecentCreated DateFilter] start={start_date}, end={end_date}, snapshot=MAX(uploadedon) subquery")
            else:
                # Use traditional idx_label-based filtering (last 2 weeks from snapshot)
                if idx_label:
                    date_condition = f"AND [Created Date] >= DATEADD(WEEK, -2, CONVERT(DATETIME, '{idx_label}', 101))"
                else:
                    date_condition = "AND [Created Date] >= DATEADD(WEEK, -2, GETDATE())"
                uploadedon_condition = f"uploadedon = '{idx_label}'"

            top_clause = '' if row_limit == 'all' else f'TOP {row_limit}'
            # Apply all filters including vertical, region, dealsize, opptype even when date filter is active
            if use_date_filter and start_date and end_date:
                recent_deals_query = f"""
                    SELECT {top_clause}
                        CAST([WinZone Opportunity ID] AS BIGINT) as [WinZone Opportunity ID],
                        [Opportunity Name],
                        ISNULL([Account Name], '') as [Account Name],
                        [R_Vertical] as Vertical,
                        [Market Unit],
                        [Sales Stage],
                        CASE 
                            WHEN [Sales Stage] IN ('3. Solutioning', '4. End-Game', '5. Negotiation') THEN 'Qualified'
                            ELSE 'Not Qualified'
                        END as [Qualified],
                        [Created Date],
                        [CloseDate] as [Close Date],
                        ROUND(ISNULL([Net TCV], 0)/1000000, 6) as [Net TCV]
                    FROM [dbo].[Pipelinedata]
                    WHERE {uploadedon_condition}
                        AND [R_Vertical] IN ('{vertical_filter}')
                        AND [Market Unit] IN ('{region_filter}')
                        AND [Sales Stage] IN ('1. Engagement', '2. Shaping', '3. Solutioning', '4. End-Game', '5. Negotiation')
                        AND [DealSize] IN ('{dealsize_filter}')
                        AND [R_Deal Type] IN ('{opptype_filter}')
                        {"AND [Account Name] IN ('" + "','".join(account_name.split(',')) + "')" if account_name and 'All' not in account_name.split(',') else ''}
                        {bu_filter_sql}
                        {sbu_filter_sql}
                        {date_condition}
                    ORDER BY [Net TCV] DESC
                """
            else:
                recent_deals_query = f"""
                    SELECT {top_clause}
                        CAST([WinZone Opportunity ID] AS BIGINT) as [WinZone Opportunity ID],
                        [Opportunity Name],
                        ISNULL([Account Name], '') as [Account Name],
                        [R_Vertical] as Vertical,
                        [Market Unit],
                        [Sales Stage],
                        CASE 
                            WHEN [Sales Stage] IN ('3. Solutioning', '4. End-Game', '5. Negotiation') THEN 'Qualified'
                            ELSE 'Not Qualified'
                        END as [Qualified],
                        [Created Date],
                        [CloseDate] as [Close Date],
                        ROUND(ISNULL([Net TCV], 0)/1000000, 6) as [Net TCV]
                    FROM [dbo].[Pipelinedata]
                    WHERE {uploadedon_condition}
                        AND [R_Vertical] IN ('{vertical_filter}')
                        AND [Market Unit] IN ('{region_filter}')
                        AND [Sales Stage] IN ('1. Engagement', '2. Shaping', '3. Solutioning', '4. End-Game', '5. Negotiation')
                        AND [DealSize] IN ('{dealsize_filter}')
                        AND [R_Deal Type] IN ('{opptype_filter}')
                        {"AND [Account Name] IN ('" + "','".join(account_name.split(',')) + "')" if account_name and 'All' not in account_name.split(',') else ''}
                        {bu_filter_sql}
                        {sbu_filter_sql}
                        {date_condition}
                    ORDER BY [Net TCV] DESC
                """
            print(f"[RecentCreated] Executing query with uploadedon_condition={uploadedon_condition}, use_date_filter={use_date_filter}")
            cursor.execute(recent_deals_query)
            col_names = [col_desc[0] for col_desc in cursor.description]
            recentDealsDf = DataFrame.from_records(cursor.fetchall(), columns=col_names)
            print(f"[RecentCreated] Query returned {len(recentDealsDf)} rows")
            
            # Format Created Date and Close Date to string for JSON serialization
            if not recentDealsDf.empty:
                if 'Created Date' in recentDealsDf.columns:
                    recentDealsDf['Created Date'] = pd.to_datetime(recentDealsDf['Created Date'], errors='coerce').dt.strftime('%m/%d/%Y')
                if 'Close Date' in recentDealsDf.columns:
                    recentDealsDf['Close Date'] = pd.to_datetime(recentDealsDf['Close Date'], errors='coerce').dt.strftime('%m/%d/%Y')
            recentDealsData = recentDealsDf.to_dict(orient="records")
        
        return JsonResponse({'success': True, 'data': recentDealsData})
    except Exception as e:
        print(f"Error in AjaxCallForRecentCreatedDeals: {e}")
        traceback.print_exc()
        return JsonResponse({'success': False, 'error': str(e), 'data': []})


def AjaxCallForRecentLastModifiedDeals(request):
    """AJAX endpoint to fetch Recent Last Modified Deals with configurable row limit and optional date range filter"""
    try:
        row_limit_raw = request.GET.get('row_limit', '20')
        if row_limit_raw.lower() == 'all':
            row_limit = 'all'
        else:
            row_limit = int(row_limit_raw)
            if row_limit not in [10, 20, 30, 50, 100, 300]:
                row_limit = 20
        
        idx_label = request.GET.get('idx_label', '')
        vertical = request.GET.get('vertical', '')
        region = request.GET.get('region', '')
        dealsize = request.GET.get('dealsize', '')
        opptype = request.GET.get('opptype', '')
        account_name = request.GET.get('account', '')
        bu = request.GET.get('bu', '')
        sbu = request.GET.get('sbu', '')
        
        # New date range filter parameters
        use_date_filter = request.GET.get('use_date_filter', 'false') == 'true'
        start_date = request.GET.get('start_date', '')
        end_date = request.GET.get('end_date', '')
        
        with connection.cursor() as cursor:
            vertical_filter = "','".join(vertical.split(',')) if vertical else ''
            region_filter = "','".join(region.split(',')) if region else ''
            dealsize_filter = "','".join(dealsize.split(',')) if dealsize else ''
            opptype_filter = "','".join(opptype.split(',')) if opptype else ''
            
            # Build BU and SBU filter conditions
            bu_values = [v.strip() for v in bu.split(',') if v.strip()] if bu else []
            if bu_values and 'All' not in bu_values:
                bu_filter_sql = "AND [BU] IN ('" + "','".join(bu_values) + "')"
            else:
                bu_filter_sql = ''
            
            sbu_values = [v.strip() for v in sbu.split(',') if v.strip()] if sbu else []
            if sbu_values and 'All' not in sbu_values:
                sbu_filter_sql = "AND [SBU1] IN ('" + "','".join(sbu_values) + "')"
            else:
                sbu_filter_sql = ''
            
            # Build date condition based on filter mode
            if use_date_filter and start_date and end_date:
                # Use custom date range filter - for Last Modified table, filter by Last Modified Date
                # Use DATE cast to ignore time components for proper date-only comparison
                date_condition = f"AND CAST([Last Modified Date] AS DATE) >= CONVERT(DATE, '{start_date}', 101) AND CAST([Last Modified Date] AS DATE) <= CONVERT(DATE, '{end_date}', 101)"
                # Custom date range may extend beyond the selected idx_label snapshot,
                # so always use the LATEST snapshot. Use a SQL subquery to avoid
                # Python-side type/format conversion issues with the uploadedon column.
                uploadedon_condition = "uploadedon = (SELECT MAX(uploadedon) FROM [dbo].[Pipelinedata])"
                print(f"[LastModified DateFilter] start={start_date}, end={end_date}, snapshot=MAX(uploadedon) subquery")
            else:
                # Use traditional idx_label-based filtering (last 2 weeks from snapshot)
                if idx_label:
                    date_condition = f"AND [Last Modified Date] >= DATEADD(WEEK, -2, CONVERT(DATETIME, '{idx_label}', 101))"
                else:
                    date_condition = "AND [Last Modified Date] >= DATEADD(WEEK, -2, GETDATE())"
                uploadedon_condition = f"uploadedon = '{idx_label}'"

            top_clause = '' if row_limit == 'all' else f'TOP {row_limit}'
            # Apply all filters including vertical, region, dealsize, opptype even when date filter is active
            if use_date_filter and start_date and end_date:
                last_modified_query = f"""
                    SELECT {top_clause}
                        CAST([WinZone Opportunity ID] AS BIGINT) as [WinZone Opportunity ID],
                        [Opportunity Name],
                        ISNULL([Account Name], '') as [Account Name],
                        [R_Vertical] as Vertical,
                        [Market Unit],
                        [Sales Stage],
                        CASE WHEN [Qualified] = 1 THEN 'Qualified' ELSE 'Not Qualified' END as [Qualified],
                        [Last Modified Date],
                        [CloseDate] as [Close Date],
                        ROUND(ISNULL([Net TCV], 0)/1000000, 6) as [Net TCV]
                    FROM [dbo].[Pipelinedata]
                    WHERE {uploadedon_condition}
                        AND [R_Vertical] IN ('{vertical_filter}')
                        AND [Market Unit] IN ('{region_filter}')
                        AND [Sales Stage] IN ('1. Engagement', '2. Shaping', '3. Solutioning', '4. End-Game', '5. Negotiation')
                        AND [DealSize] IN ('{dealsize_filter}')
                        AND [R_Deal Type] IN ('{opptype_filter}')
                        {"AND [Account Name] IN ('" + "','".join(account_name.split(',')) + "')" if account_name and 'All' not in account_name.split(',') else ''}
                        {bu_filter_sql}
                        {sbu_filter_sql}
                        {date_condition}
                    ORDER BY [Net TCV] DESC
                """
            else:
                last_modified_query = f"""
                    SELECT {top_clause}
                        CAST([WinZone Opportunity ID] AS BIGINT) as [WinZone Opportunity ID],
                        [Opportunity Name],
                        ISNULL([Account Name], '') as [Account Name],
                        [R_Vertical] as Vertical,
                        [Market Unit],
                        [Sales Stage],
                        CASE WHEN [Qualified] = 1 THEN 'Qualified' ELSE 'Not Qualified' END as [Qualified],
                        [Last Modified Date],
                        [CloseDate] as [Close Date],
                        ROUND(ISNULL([Net TCV], 0)/1000000, 6) as [Net TCV]
                    FROM [dbo].[Pipelinedata]
                    WHERE {uploadedon_condition}
                        AND [R_Vertical] IN ('{vertical_filter}')
                        AND [Market Unit] IN ('{region_filter}')
                        AND [Sales Stage] IN ('1. Engagement', '2. Shaping', '3. Solutioning', '4. End-Game', '5. Negotiation')
                        AND [DealSize] IN ('{dealsize_filter}')
                        AND [R_Deal Type] IN ('{opptype_filter}')
                        {"AND [Account Name] IN ('" + "','".join(account_name.split(',')) + "')" if account_name and 'All' not in account_name.split(',') else ''}
                        {bu_filter_sql}
                        {sbu_filter_sql}
                        {date_condition}
                    ORDER BY [Net TCV] DESC
                """
            print(f"[LastModified] Executing query with uploadedon_condition={uploadedon_condition}, use_date_filter={use_date_filter}")
            cursor.execute(last_modified_query)
            col_names = [col_desc[0] for col_desc in cursor.description]
            lastModifiedDf = DataFrame.from_records(cursor.fetchall(), columns=col_names)
            print(f"[LastModified] Query returned {len(lastModifiedDf)} rows")
            
            # Format Last Modified Date and Close Date to string for JSON serialization
            if not lastModifiedDf.empty:
                if 'Last Modified Date' in lastModifiedDf.columns:
                    lastModifiedDf['Last Modified Date'] = pd.to_datetime(lastModifiedDf['Last Modified Date'], errors='coerce').dt.strftime('%m/%d/%Y')
                if 'Close Date' in lastModifiedDf.columns:
                    lastModifiedDf['Close Date'] = pd.to_datetime(lastModifiedDf['Close Date'], errors='coerce').dt.strftime('%m/%d/%Y')
            lastModifiedData = lastModifiedDf.to_dict(orient="records")
        
        return JsonResponse({'success': True, 'data': lastModifiedData})
    except Exception as e:
        print(f"Error in AjaxCallForRecentLastModifiedDeals: {e}")
        traceback.print_exc()
        return JsonResponse({'success': False, 'error': str(e), 'data': []})


def getDropDownValues(idx_label_selected):
    cursor = connection.cursor()
    
    # Optimized: Skip DataFrame creation for simple DISTINCT queries - extract values directly
    cursor.execute("select distinct R_Vertical from pipelinedata where R_Vertical is not null and uploadedon='"+idx_label_selected+"'")
    verticalData = [row[0] for row in cursor.fetchall()]

    cursor.execute("select distinct [Market Unit] from pipelinedata where [Market Unit] is not null and uploadedon='"+idx_label_selected+"'")
    marketData_all = [row[0] for row in cursor.fetchall()]
    marketData = [mu for mu in marketData_all if mu in ALLOWED_MARKET_UNITS]
    
    cursor.execute("select distinct [BU] from pipelinedata where [BU] is not null and uploadedon='"+idx_label_selected+"'")
    butData = [row[0] for row in cursor.fetchall()]
    butData.insert(0,'All')

    cursor.execute("select distinct [SBU1] from pipelinedata where [SBU1] is not null and uploadedon='"+idx_label_selected+"'")
    sbuData = [row[0] for row in cursor.fetchall()]
    sbuData.insert(0,'All')

    cursor.execute("select distinct [Account Name] from pipelinedata where [Account Name] is not null and uploadedon='"+idx_label_selected+"'")
    accountData = [row[0] for row in cursor.fetchall()]
    accountData.insert(0,'All')

    cursor.execute("select distinct [Account Tagging] from pipelinedata where [Account Tagging] is not null and uploadedon='"+idx_label_selected+"'")
    accountTagData = [row[0] for row in cursor.fetchall()]

    cursor.execute("select distinct [R_Deal Type] from pipelinedata where [R_Deal Type] is not null and uploadedon='"+idx_label_selected+"'")
    opportunityData = [row[0] for row in cursor.fetchall()]

    cursor.execute("select distinct [Sales Stage] from pipelinedata where [Sales Stage] is not null and uploadedon='"+idx_label_selected+"'")
    salesstageData = [row[0] for row in cursor.fetchall()]
    salesstageData.sort()

    cursor.execute("select distinct Practice from pipelinedata where [Practice] is not null and uploadedon='"+idx_label_selected+"'")
    lst_horizontal_raw = [row[0] for row in cursor.fetchall()]
    lst_horizontal_set = set()
    for value in lst_horizontal_raw:
        if value is not None:
            for item in value.split(';'):
                item = item.strip()
                if item and item != 'null':
                    lst_horizontal_set.add(item)
    horizontalData = ["All"] + sorted(lst_horizontal_set)

    cursor.execute("select distinct [Market Unit],[BU],[SBU1],[Opportunity Source] ,[Account Type] from pipelinedata where uploadedon='"+idx_label_selected+"'")
    resultDepData = cursor.fetchall()
    try:
        resultDepData = filter(lambda row: str(row[2]) != "0" and str(row[-1]) != "NULL", resultDepData)
        resultDepData = filter(lambda row: row[0] in ALLOWED_MARKET_UNITS, resultDepData)
        resultDepData = [
            [val.strip() if type(val) == str else val for val in row] for row in resultDepData
        ]
    except:
        resultDepData = []
    return verticalData,marketData,butData,sbuData,accountData,accountTagData,horizontalData,opportunityData,salesstageData,resultDepData

def getIndexLabels():
    cursor = connection.cursor()
    cursor.execute("select distinct UploadedOn from pipelinedata ORDER BY UploadedOn DESC")
    data = [pd.to_datetime(row[0]).strftime('%m/%d/%Y') for row in cursor.fetchall()]
    return data

def show_dashboard(request):
    index_labels = getIndexLabels()
    idx_label_selected = request.GET.get("idx_label")
    if idx_label_selected == None:
        idx_label_selected = index_labels[0]
    print('idx_label_selected',idx_label_selected)
    values = getDropDownValues(idx_label_selected)
    selected_values = parse_selected_values(request,values[0],values[1],values[7],values[5],index_labels[0])
    # selected_values['idx_label_selected'] = datetime.strptime(selected_values['idx_label_selected'],'%b%d%Y').strftime('%m/%d/%Y')

    opt_opp_vertical_new = request.GET.getlist("vertical[]")
    if opt_opp_vertical_new == []:
        opt_opp_vertical_new = values[0]
    opt_opp_region_new = request.GET.getlist("unq_region[]")
    if opt_opp_region_new == []:
        opt_opp_region_new = values[1]
    
    # --- UPDATE THIS BLOCK (Default to 5 Active Stages) ---
    stages = request.GET.getlist("sales_stage[]")
    cleaned = [s for s in stages if s != "Duplicate"]
    opt_sales_stage = cleaned
    if opt_sales_stage == []:
        opt_sales_stage = ['1. Engagement', '2. Shaping', '3. Solutioning', '4. End-Game', '5. Negotiation']
        
    opt_deal_size = request.GET.getlist("deal_size[]")
    if opt_deal_size == []:
            opt_deal_size = ["$0m - $2.5m", "$2.5m - $10m", "$10m - $25m", "$25m - $50m", ">= $50m"]
    opt_tier_data = request.GET.getlist("tier_data[]")
    if opt_tier_data ==[]:
        opt_tier_data = values[5]
    opt_opp_type = request.GET.getlist("type[]")
    if opt_opp_type == []:
        opt_opp_type = values[7]
    opt_mcu_checked = request.GET.getlist("unique_mcu[]")
    if opt_mcu_checked == []:
        opt_mcu_checked = ["All"]
    opt_sbu_checked = request.GET.getlist("unique_sbu[]")
    if opt_sbu_checked == []:
        opt_sbu_checked = ["All"]
    opt_account_name_checked = request.GET.getlist("unique_account_name[]")
    if opt_account_name_checked == []:
        opt_account_name_checked = ["All"]
    opt_horizontal_checked = request.GET.getlist("unique_horizontal[]")
    if opt_horizontal_checked == []:
        opt_horizontal_checked = ["All"]
    
    sp_cl_stmt = "EXEC [dbo].[get_PipelineReport_Dev] @ptype= %s,@pvertical= %s,@pRegion= %s,@pBU= %s,@pSBU= %s,@pAccount= %s,@pHorizontal= %s,@pOpportunity= %s,@pSalesStage= %s,@pDealSize= %s,@pTier= %s,@ReportDt= %s,@WinzoneID=%s"
    common_args = (
        selected_values["selected_vertical"],
        selected_values["selected_region"],
        selected_values["selected_bu"],
        selected_values["selected_sbu"],
        selected_values["selected_account"],
        'All',
        selected_values["selected_oppType"],
        selected_values["selected_salesstage"],
        selected_values["selected_dealsize"],
        selected_values["selected_tier"],
        selected_values['idx_label_selected'],
        selected_values['selected_winzoneID']
    )
    common_args2 = (
        selected_values["selected_vertical"],
        selected_values["selected_region"],
        selected_values["selected_bu"],
        selected_values["selected_sbu"],
        selected_values["selected_account"],
        'All',
        selected_values["selected_oppType"],
        'Lost,Won',
        selected_values["selected_dealsize"],
        selected_values["selected_tier"],
        selected_values['idx_label_selected'],
        selected_values['selected_winzoneID']
    )
    common_args3 = (
        selected_values["selected_vertical"],
        selected_values["selected_region"],
        selected_values["selected_bu"],
        selected_values["selected_sbu"],
        selected_values["selected_account"],
        'All',
        selected_values["selected_oppType"],
        'All',
        selected_values["selected_dealsize"],
        selected_values["selected_tier"],
        selected_values['idx_label_selected'],
        selected_values['selected_winzoneID']
    )
    sp_cl_stmt_dev = "EXEC [dbo].[get_PipelineReport_Dev] @ptype=%s,@pvertical=%s,@pRegion=%s,@pBU=%s,@pSBU=%s,@pAccount=%s,@pHorizontal=%s,@pOpportunity=%s,@pSalesStage=%s,@pDealSize=%s,@pTier=%s,@ReportDt=%s,@WinzoneID=%s"
    common_args_dev = (
        selected_values["selected_vertical"],
        selected_values["selected_region"],
        selected_values["selected_bu"],
        selected_values["selected_sbu"],
        selected_values["selected_account"],
        'All',
        selected_values["selected_oppType"],
        selected_values["selected_salesstage"],
        selected_values["selected_dealsize"],
        selected_values["selected_tier"],
        selected_values['idx_label_selected'],
        ''
        # selected_values['selected_winzoneID']
    )
    common_args_funnel = (
        selected_values["selected_vertical"],
        selected_values["selected_region"],
        selected_values["selected_bu"],
        selected_values["selected_sbu"],
        selected_values["selected_account"],
        'All',
        selected_values["selected_oppType"],
        'All',
        selected_values["selected_dealsize"],
        selected_values["selected_tier"],
        selected_values['idx_label_selected'],
        selected_values['selected_winzoneID']
    )
    with connection.cursor() as cursor:
        # cursor = connection.cursor()
        # cursor.execute(sp_cl_stmt_dev, ("getUnique_Winzone",) + common_args_dev)
        # col_names = [col_desc[0] for col_desc in cursor.description]
        # windf = DataFrame.from_records(cursor.fetchall(), columns=col_names)
        # windata = windf['WinZone Opportunity ID'].values.tolist()
        try:
            cursor = connection.cursor()
            cursor.execute(sp_cl_stmt, ("getTotal_TCV_Count",) + common_args)
            col_names = [col_desc[0] for col_desc in cursor.description]
            dfTotal = DataFrame.from_records(cursor.fetchall(), columns=col_names)
            dfTotalData = dfTotal.to_dict(orient="records")
            df_count = "{:,.0f}".format(int(dfTotalData[0]['WinZone Opportunity ID']))
            tcv_total = "{:,.1f}".format(dfTotalData[0]['TCV'])
        except:
            dfTotalData=[]
            df_count=0
            tcv_total=0

        try:
            cursor = connection.cursor()
            cursor.execute(sp_cl_stmt_dev, ("FunnelData",) + common_args_funnel)
            col_names = [col_desc[0] for col_desc in cursor.description]
            funneldf = DataFrame.from_records(cursor.fetchall(), columns=col_names)
            # funneldf = funneldf.sort_values(by='TCV',ascending=False)
            funnelData = funneldf.to_dict(orient="records")
        except:
            funnelData=[]

        try:
            cursor = connection.cursor()
            cursor.execute(sp_cl_stmt, ("ByVert_Region_Stage",) + common_args)
            # cursor.execute("get_Pipeline @ptype = ByVert_Region_Stage") #sp_call_stmt2,('Base',)+common_args
            col_names = [col_desc[0] for col_desc in cursor.description]
            df = DataFrame.from_records(cursor.fetchall(), columns=col_names)

            verticaldf=df.groupby(["Vertical"], as_index=False).agg({ "TCV":sum })
            verticaldf['TCV'] = verticaldf['TCV'].round(1)
            verticalData = verticaldf.to_dict(orient="records")
            vertical = verticaldf['Vertical'].values.tolist()
            tcv = verticaldf['TCV'].values.tolist()

            regiondf=df.groupby(["Region"], as_index=False).agg({ "TCV":sum })
            regiondf['TCV'] = regiondf['TCV'].round(1)
            regionData = regiondf.to_dict(orient="records")
            Region=regiondf['Region'].values.tolist()
            tcv2=regiondf['TCV'].values.tolist()
            
            salesstagedf=df.groupby(["Sales Stage"], as_index=False).agg({ "TCV":sum })
            salesstagedf['TCV'] = salesstagedf['TCV'].round(1)
            salesstageData = salesstagedf.to_dict(orient="records")
            SalesStage=salesstagedf['Sales Stage'].values.tolist()
            tcv3=salesstagedf['TCV'].values.tolist()

            buDf=df.groupby(["BU"], as_index=False).agg({ "TCV":sum })
            buDf['TCV'] = buDf['TCV'].round(1)
            butableData = buDf.to_dict(orient="records")
            buData=buDf['BU'].values.tolist()
            tcv4=buDf['TCV'].values.tolist()
        except:
            verticalData=[]
            vertical=[]
            tcv=[]
            regionData=[]
            Region=[]
            tcv2=[]
            salesstageData=[]
            SalesStage=[]
            tcv3=[]
            butableData=[]
            buData=[]
            tcv4=[]
        try:
            cursor = connection.cursor()
            cursor.execute(sp_cl_stmt, ("SalesStage_ByQtr",) + common_args)
            # cursor.execute("get_Pipeline @ptype = SalesStage_ByQtr") #sp_call_stmt2,('Base',)+common_args
            col_names = [col_desc[0] for col_desc in cursor.description]
            df2 = DataFrame.from_records(cursor.fetchall(), columns=col_names)
            ssData = df2
            df2Data = df2.groupby(["Actual Close Date","Sales Stage"], as_index=False).agg(
                { "TCV":sum,
                "WinZone Opportunity ID": "count",
                }
            )
            df2Data['TCV'] = df2Data['TCV'].round(1)
            salesStageByQtr = df2Data.to_dict(orient="records")

            df5 = ssData.groupby(["Sales Stage","BU"], as_index=False).agg(
                { "TCV":sum,
                "WinZone Opportunity ID": "count",
                }
            )
            df5['TCV'] = df5['TCV'].round(1)
            sbuBySS = df5.to_dict(orient="records")
        except:
            salesStageByQtr=[]
            sbuBySS=[]
        
        try:
            cursor = connection.cursor()        
            cursor.execute(sp_cl_stmt, ("DealSize_ByQtr",) + common_args)
            # cursor.execute("get_Pipeline @ptype = DealSize_ByQtr") #sp_call_stmt2,('Base',)+common_args
            col_names = [col_desc[0] for col_desc in cursor.description]
            df3 = DataFrame.from_records(cursor.fetchall(), columns=col_names)

            df3Data = df3.groupby(["Actual Close Date","DealSize"], as_index=False).agg(
                { "TCV":sum,
                "WinZone Opportunity ID": "count",
                }
            )
            df3Data['TCV'] = df3Data['TCV'].round(1)
            df3Data = df3Data.sort_values(by=['Actual Close Date'],ascending=True)
            dealSizeByQtr = df3Data.to_dict(orient="records")
        except:
            dealSizeByQtr=[]

        # cursor.execute("select top 1000 [Account Name],R_Vertical Vertical,[Opportunity Record Type Name],[Sales Stage],SBU1,[WinZone Opportunity ID],round([Gross TCV $]/1000000,2) as TCV,CloseMonth [Close Date] from pipelinedata")
        if selected_values["selected_salesstage"] == 'Lost,Won':
            try:
                # cursor = connection.cursor()
                # cursor.execute(sp_cl_stmt, ("WonLostDetails",) + common_args2)
                # col_names = [col_desc[0] for col_desc in cursor.description]
                # df4 = DataFrame.from_records(cursor.fetchall(), columns=col_names)
                # df4['TCV'] = df4['TCV'].round(1)
                # wonlostdf = df4
                # wonlostData = wonlostdf.to_dict(orient="records")
                # dealInPipeline = []
                # accountData = []
                wonlostData =[]
                dealInPipeline = []
                accountData = []
            except:
                wonlostData =[]
                dealInPipeline = []
                accountData = []
        else:
            with connection.cursor() as cursor:
            # cursor = connection.cursor()
                try:
                    # cursor.execute(sp_cl_stmt, ("WonLostDetails",) + common_args3)
                    # col_names = [col_desc[0] for col_desc in cursor.description]
                    # df4 = DataFrame.from_records(cursor.fetchall(), columns=col_names)
                    # df4['TCV'] = df4['TCV'].round(1)
                    # wonlostdf = df4
                    # wonlostdf = wonlostdf[(wonlostdf["Sales Stage"].isin(['Lost','Won']))]
                    # wonlostData = wonlostdf.to_dict(orient="records")

                    # selectSalesValues = request.GET.getlist("sales_stage[]")or ["3. Solutioning", "4. End-Game", "5. Negotiation"]
                    # dealinpipeDf = df4
                    # dealinpipeDf = dealinpipeDf[(dealinpipeDf["Sales Stage"].isin(selectSalesValues))]
                    # dealInPipeline = dealinpipeDf.to_dict(orient="records")
                    
                    # accountData = dealInPipeline
                    wonlostData = [] 
                    dealInPipeline = []
                    accountData = []
                except:
                    wonlostData = [] 
                    dealInPipeline = []
                    accountData = []

    # cursor.execute(sp_cl_stmt, ("deals_in_pipeline",) + common_args)
    # col_names = [col_desc[0] for col_desc in cursor.description]
    # dealsdf = DataFrame.from_records(cursor.fetchall(), columns=col_names)
    # dealInPipeline = dealsdf.to_dict(orient="records")
    # dealInPipeline=[]
    

    # cursor.execute(sp_cl_stmt, ("dashboard_accounts",) + common_args)
    # # cursor.execute("get_Pipeline @ptype = SalesStage_ByQtr") #sp_call_stmt2,('Base',)+common_args
    # col_names = [col_desc[0] for col_desc in cursor.description]
    # df6 = DataFrame.from_records(cursor.fetchall(), columns=col_names)
    # accountDf = df6
    # accountData = accountDf.to_dict(orient="records")
    # accountData=[]
    
    # Top 20 Deals by Net TCV
    try:
        with connection.cursor() as cursor:
            # Build the filter conditions based on selected values
            vertical_filter = "','".join(selected_values["selected_vertical"].split(','))
            region_filter = "','".join(selected_values["selected_region"].split(','))
            salesstage_filter = "','".join(selected_values["selected_salesstage"].split(','))
            dealsize_filter = "','".join(selected_values["selected_dealsize"].split(','))
            opptype_filter = "','".join(selected_values["selected_oppType"].split(','))
            tier_filter = "','".join(selected_values["selected_tier"].split(','))
            
            top20_query = f"""
                SELECT TOP 20 
                    [WinZone Opportunity ID],
                    [Opportunity Name],
                    [Account Name],
                    [R_Vertical] as Vertical,
                    [Market Unit],
                    [Sales Stage],
                    CASE WHEN [Qualified] = 1 THEN 'Qualified' ELSE 'Not Qualified' END as [Qualified],
                    [R_Deal Type],
                    [CloseDate] as [Close Date],
                    ROUND(ISNULL([Net TCV], 0)/1000000, 6) as [Net TCV]
                FROM [dbo].[Pipelinedata]
                WHERE uploadedon = '{selected_values['idx_label_selected']}'
                    AND [R_Vertical] IN ('{vertical_filter}')
                    AND [Market Unit] IN ('{region_filter}')
                    AND [Sales Stage] IN ('{salesstage_filter}')
                    AND [DealSize] IN ('{dealsize_filter}')
                    AND [R_Deal Type] IN ('{opptype_filter}')
                ORDER BY [Net TCV] DESC
            """
            cursor.execute(top20_query)
            col_names = [col_desc[0] for col_desc in cursor.description]
            top20Df = DataFrame.from_records(cursor.fetchall(), columns=col_names)
            # Ensure date columns are serialized as strings (MM/DD/YYYY) to avoid JSON errors
            if not top20Df.empty:
                if 'Close Date' in top20Df.columns:
                    top20Df['Close Date'] = pd.to_datetime(top20Df['Close Date'], errors='coerce').dt.strftime('%m/%d/%Y')
            top20DealsData = top20Df.to_dict(orient="records")
    except Exception as e:
        print(f"Error fetching top 20 deals: {e}")
        top20DealsData = []
    
    return render(request, 'visualize/show_dashboard_new.html', context={
        'plot_label':"Dashboard",
        'winzone_id':selected_values['selected_winzoneID'],
        # 'windata':json.dumps(windata),
        'funnelData':json.dumps(funnelData),
        "opt_sales_stage_new":json.dumps(opt_sales_stage),
        "df_count":df_count,"tcv_total":tcv_total,
        'index_labels':index_labels,
        'idx_label_selected':idx_label_selected,
        "vertical":json.dumps(vertical),
        "tcv":json.dumps(tcv),
        "Region":json.dumps(Region),
        "tcv2":json.dumps(tcv2),
        "SalesStage":json.dumps(SalesStage),
        "tcv3":json.dumps(tcv3),
        "buData":json.dumps(buData),
        "tcv4":json.dumps(tcv4),
        "salesStageByQtr":json.dumps(salesStageByQtr),
        "dealSizeByQtr":json.dumps(dealSizeByQtr),
        "wonlostData":json.dumps(wonlostData),
        "sbuBySS":json.dumps(sbuBySS),
        "dealInPipeline":json.dumps(dealInPipeline),
        "accountData":json.dumps(accountData),
        "vertical_unique":values[0],
        "region_unique":values[1],
        "unique_mcu":values[2],
        "unique_sbu":values[3],
        "unique_account_name":values[4],
        "unique_horizontal":values[6],
        "sales_stage":values[8],
        "opp_type":values[7],
        "deal_size":["$0m - $2.5m", "$2.5m - $10m", "$10m - $25m", "$25m - $50m", ">= $50m"],
        "tier_data":values[5],
        "opt_opp_vertical_new":opt_opp_vertical_new,
        "opt_opp_region_new":opt_opp_region_new,
        "opt_sales_stage":opt_sales_stage,
        "opt_deal_size":opt_deal_size,
        "opt_mcu_checked":opt_mcu_checked,
        "opt_sbu_checked":opt_sbu_checked,
        "opt_account_name_checked":opt_account_name_checked,
        "opt_horizontal_checked":opt_horizontal_checked,
        "opt_opp_type":opt_opp_type,
        "opt_tier_data":opt_tier_data,

        "verticalData":json.dumps(verticalData),
        "regionData":json.dumps(regionData),
        "salesstageData":json.dumps(salesstageData),
        "butableData":json.dumps(butableData),
        "resultDepData":json.dumps(values[9]),
        "top20DealsData":json.dumps(top20DealsData),
        **selected_values,
        "opt_opp_region_new_filter":json.dumps(opt_opp_region_new)
    })
# def insert_chunk(chunk,tablename):
#     try:
#         cursor = connection.cursor()
#         chunk = chunk.copy()
#         chunk.replace({np.nan:None},inplace=True)
#         data = [tuple(row) for row in chunk.itertuples(index=False,name=None)]
#         columnnames = ','.join(chunk.columns)
#         print(data)
#         print(columnnames)
#         print("SqlQuery",len(chunk.columns))
#         # placeholder=",".join(["%s"]*262) #','.join(["%s" for _ in chunk.columns])
#         # sql=f"insert into {tablename} ({columnnames}) values ({placeholder})"
#         # print("SQL 20:",len(chunk.columns),len(data[0]))
#         # cursor.executemany(sql,data)
#         # connection.commit()
#     except Exception as e:
#         print("527:",e)
# def insert_chunk(chunk, tablename):
#     print(tablename)
#     try:
#         cursor = connection.cursor()
#         # Step 1: Get actual column names from DB table
#         cursor.execute("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = ?", (tablename,))
#         table_columns = set(row[0] for row in cursor.fetchall())
       
#         # Step 2: Filter only matching columns
#         chunk = chunk.copy()
#         chunk_columns = [col for col in chunk.columns if col in table_columns]
#         chunk = chunk[chunk_columns]
#         chunk['UploadedOn'] = pd.to_datetime(chunk['UploadedOn'], errors='coerce')
#         # chunk['Expected Revenue Start Date'] = pd.to_datetime(chunk['Expected Revenue Start Date'], errors='coerce')
#         # chunk['Estimated Deal Close Date'] = pd.to_datetime(chunk['Estimated Deal Close Date'], errors='coerce')
#         # chunk['Actual Close Date'] = pd.to_datetime(chunk['Actual Close Date'], errors='coerce')
#         # chunk['Created Date'] = pd.to_datetime(chunk['Created Date'], errors='coerce')
#         # chunk['Last Modified Date'] = pd.to_datetime(chunk['Last Modified Date'], errors='coerce')

#         chunk.replace(['-', '', 'nan', 'NaN', 'N/A'], np.nan, inplace=True)

#         # possible_numeric_cols = ["Confidence %", "Win Probability (%)", "Customer Profitability (%)",
#         #                          "Gross TCV $", "Total Horizontal TCV", "Net TCV", "CY Q1 $", "CY Q2 $",
#         #                          "CY Q3 $", "CY Q4 $", "Forecast First Year Amount", "InceFrstYearAmt",
#         #                          "Customer Profitability Amount","RevenueMoM","Deal Duration (Months)",
#         #                          "WinZone Opportunity ID","OwnerEmpID","Customer ID","Proactive Engagement",
#         #                         "Qualified","Current Year Revenue","Next Year Revenue Forecast","Current Year Revenue Forecast",
#         #                         "Next Year Revenue","Next Year Revenue (converted)","Days in Stage","Strategic Account",
#         #                         "Age","Partner Supported","Partner","# Service Lines",
#         #                         "Framework Total Value","ASK Response Completed?","3rd Party Advisor (TPA)","Advisor Company: Account Name",
#         #                         "Opportunity: CY REVENUE $","InceFrstYearAmt","Forecast First Year Amount"]


#         # for col in chunk.columns:
#         #     if col in possible_numeric_cols:
#         #         # Replace common invalid placeholders with np.nan first
#         #         chunk[col] = chunk[col].replace(['-', '', 'nan', 'NaN', 'N/A'], np.nan)
            
#         #         # Convert to numeric (coerce invalids to NaN)
#         #         chunk[col] = pd.to_numeric(chunk[col], errors='coerce')

#         #         # Optional: Round to 5 decimal places to fit precision
#         #         chunk[col] = chunk[col].round(5)
       
#         chunk = chunk.where(pd.notnull(chunk),None) 

#         for i,col in enumerate(chunk.columns,1):
#             print("{}: {}".format(i,col))

#         print("579=======",chunk.dtypes)
#         print("580=======",chunk.iloc[0])
       
#         # Step 4: Prepare insert
#         data = [tuple(row) for row in chunk.itertuples(index=False, name=None)]

#         # columnnames = ', '.join(f'"{col.replace("\"", "\"\"")}"' for col in chunk.columns)
#         escaped_columns = [col.replace('"', '""') for col in chunk.columns]
#         columnnames = ', '.join(['"{}"'.format(col) for col in escaped_columns])
#         placeholder = ', '.join(['?'] * len(escaped_columns))
#         # sql = f'INSERT INTO "{tablename}" ({columnnames}) VALUES ({placeholder})'
#         sql = 'INSERT INTO "{}" ({}) VALUES ({})'.format(tablename,columnnames,placeholder)
       
#         # Debug prints
#         print("Filtered Columns:", chunk.columns.tolist())
#         print("Insert SQL:", sql)
#         print("Number of Rows:", len(data))
#         print("sample data",data[:1])
       
#         # Step 5: Execute insert
#         if data:
#             cursor.executemany(sql, data)
#             connection.commit()


#     except Exception as e:
#         print("527:", e)

def insert_chunk(chunk, tablename):
    print(f"Inserting data into table: {tablename}")
    try:
        cursor = connection.cursor()

        # Get actual column names from the table
        cursor.execute(f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{tablename}'")
        table_columns = set(row[0] for row in cursor.fetchall())

        # Filter only columns that exist in the table
        chunk = chunk.copy()
        chunk_columns = [col for col in chunk.columns if col in table_columns]
        chunk = chunk[chunk_columns]

        # Clean and preprocess data
        chunk['UploadedOn'] = pd.to_datetime(chunk['UploadedOn']).dt.strftime('%m/%d/%Y') # pd.to_datetime(chunk.get('UploadedOn'), errors='coerce')
        #chunk['UploadedOn'] = chunk #['UploadedOn'].dt.to_pydatetime()# pd.to_datetime(chunk['UploadedOn'], errors='coerce')
        chunk['Expected Revenue Start Date'] = pd.to_datetime(chunk['Expected Revenue Start Date']).dt.strftime('%m/%d/%Y')# pd.to_datetime(chunk['Expected Revenue Start Date'], errors='coerce')
        chunk['Estimated Deal Close Date'] = pd.to_datetime(chunk['Estimated Deal Close Date']).dt.strftime('%m/%d/%Y') # pd.to_datetime(chunk['Estimated Deal Close Date'], errors='coerce')
        chunk['Actual Close Date'] = pd.to_datetime(chunk['Actual Close Date']).dt.strftime('%m/%d/%Y')#pd.to_datetime(chunk['Actual Close Date'], errors='coerce')
        chunk['Created Date'] = pd.to_datetime(chunk['Created Date']).dt.strftime('%m/%d/%Y') # pd.to_datetime(chunk['Created Date'], errors='coerce')
        chunk['Last Modified Date'] = pd.to_datetime(chunk['Last Modified Date']).dt.strftime('%m/%d/%Y') # pd.to_datetime(chunk['Last Modified Date'], errors='coerce')
        #chunk.replace(['-', '', 'nan', 'NaN', 'N/A'], np.nan, inplace=True)
        chunk = chunk.where(pd.notnull(chunk), None)

        # Rename special characters in column names
        renamed_columns = {
            col: col.replace("#", "Number").replace("$", "Dollars").replace("%", "Percent").replace("?", "")
            for col in chunk.columns
        }
        chunk.rename(columns=renamed_columns, inplace=True)

        # Build SQL dynamically
        columns = list(chunk.columns)
        placeholders = ', '.join(['?'] * len(columns))
        columnnames = ', '.join(f'"{col}"' for col in columns)
        sql = f'INSERT INTO {tablename} ({columnnames}) VALUES ({placeholders})'

        # Convert DataFrame to list of tuples
        data = [tuple(row) for row in chunk.itertuples(index=False, name=None)]

        print("Prepared SQL:", sql)
        print("Number of columns:", len(columns))
        print("Number of values in tuple:", len(data[0]))

        if data:
            cursor.executemany(sql, data)
            connection.commit()
            print("Insert successful.")
    except Exception as e:
        print("Error during insert:", e)

def import_excel_to_db(dataframedata, tablename, num_threads=4):
    try:
        chunk_size = 5000
        df = dataframedata.copy()
        print(f"Dataframe shape: {df.shape}")
        chunks = [df.iloc[i:i + chunk_size].copy() for i in range(0, df.shape[0], chunk_size)]
        print(f"Number of chunks: {len(chunks)}",chunks)

        with ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [executor.submit(insert_chunk, chunk, tablename) for chunk in chunks]
            for future in futures:
                future.result()
    except Exception as e:
        print("Error in import_excel_to_db:", e)

def show_UploadData(request):
    if request.method == 'POST' and request.FILES:
        uploadFile = request.FILES['file']
        uploadFileName = uploadFile.name
        print('uploadFile:', uploadFileName)

        # Read Excel file
        df = pd.read_excel(uploadFile, engine="openpyxl")
        df = df.reset_index(drop=True)
        print(f"Loaded dataframe with shape: {df.shape}")
        print("Columns in the dataframe:", df.columns.tolist())

        # Clean column names
        df.columns = [col.replace("'", " ") for col in df.columns]
        print(f"Cleaned columns: {df.columns.tolist()}")

        # Start DB import in a new thread
        thread = threading.Thread(target=import_excel_to_db, args=(df, 'PipelineUploaddata'))
        thread.start()


    return render(request, 'visualize/uploadView.html', context={
        'plot_label': "UploadData",
    })

# def import_excel_to_db(dataframedata,tablename,num_threads=4):
#     try:
#         chunk_size=5000
#         df =dataframedata.copy() # pd.read_excel(filepath,engine="openpyxl").copy()
#         chunks = [df.iloc[i:i + chunk_size].copy() for i in range(0,df.shape[0],chunk_size)]
#         with ThreadPoolExecutor(max_workers=num_threads) as executor:
#             futures = [executor.submit(insert_chunk,chunk,tablename) for chunk in chunks]
#             for future in futures:
#                 future.result()
#     except Exception as e:
#         print("538:",e)
# def show_UploadData(request):
#     if request.method=='POST' and request.FILES:
#         cursor = connection.cursor()
#         uploadFile = request.FILES['file']
#         uploadFileName = request.FILES['file'].name
#         print('uploadFile',uploadFile)
#         df = pd.read_excel(uploadFile,engine="openpyxl")
#         df = df.reset_index(drop=True)
#         print(df)
#         columns = list(df.columns)
#         columns = [col.replace("'"," ") if "'" in col else col for col in columns]
#         print(columns)
#         thread = threading.Thread(target=import_excel_to_db,args=(df,'PipelineUploaddata'))
#         thread.start()
#     return render(request, 'visualize/uploadView.html', context={
#         'plot_label':"UploadData",
#     })

def show_plot(request):
    ########################################## Directing the control on the basis of the Button's Name in the Home page ##########################################
    if request.GET.get("plt_label") == "Won Lost":
        print("Inside If",request.GET.get("idx_label"))
        http_res = show_won_lost_plot(request.GET.get("plt_label"), request)
        # print("Plot completed")
    elif request.GET.get("plt_label") == "Details":
        print("inside details",request.GET.get("idx_label"))
        http_res = show_pipeline_details_plot(request.GET.get("idx_label"), request.GET.get("plt_label"), request)
        print("Plot completed")
    elif request.GET.get("plt_label") == "Summary":
        print("Inside by summary")
        http_res = show_pipeline_summary_plot(request.GET.get("idx_label"), request.GET.get("plt_label"), request)
        print("Plot completed")
    elif request.GET.get("plt_label") == "SBU":
        print("Inside by sbu")
        http_res = show_pipeline_details_SBU_plot(request.GET.get("idx_label"), request.GET.get("plt_label"), request)
        # print("Plot completed")
    else:
        # if request.GET.get("plt_label") == "Accounts":
        print("Inside by accounts")
        http_res = show_pipeline_details_Acct_plot(request.GET.get("idx_label"), request.GET.get("plt_label"), request)
        # print("Plot completed")
    # else:
    #     print("inside Key Metrices")
    #     http_res = show_key_metrices_plot(request.GET.get("idx_label"), request.GET.get("plt_label"), request)
        # print("Plot Completed")
    
    return http_res

def show_pipeline_summary_plot(index,label,request):
    # values = getDropDownValues()
    index_labels = getIndexLabels()
    idx_label_selected = request.GET.get("idx_label")
    # selected_values['idx_label_selected'] = datetime.strptime(selected_values['idx_label_selected'],'%b%d%Y').strftime('%m/%d/%Y')

    if idx_label_selected == None:
        idx_label_selected = index_labels[0]
    # else:
    #     idx_label_selected = datetime.strptime(selected_values['idx_label_selected'],'%m/%d/%Y').strftime('%b%d%Y')
    values = getDropDownValues(idx_label_selected)
    selected_values = parse_selected_values(request,values[0],values[1],values[7],values[5],index_labels[0])

    opt_opp_vertical_new = request.GET.getlist("vertical[]")
    if opt_opp_vertical_new == []:
        opt_opp_vertical_new = values[0]
    opt_opp_region_new = request.GET.getlist("unq_region[]")
    if opt_opp_region_new == []:
        opt_opp_region_new = values[1]
    stages = request.GET.getlist("sales_stage[]")
    cleaned = [s for s in stages if s != "Duplicate"]
    opt_sales_stage = cleaned
    if opt_sales_stage == []:
        opt_sales_stage = ["3. Solutioning", "4. End-Game", "5. Negotiation"]
    opt_deal_size = request.GET.getlist("deal_size[]")
    if opt_deal_size == []:
            opt_deal_size = ["$0m - $2.5m", "$2.5m - $10m", "$10m - $25m", "$25m - $50m", ">= $50m"]
    opt_tier_data = request.GET.getlist("tier_data[]")
    if opt_tier_data ==[]:
        opt_tier_data = values[5]
    opt_opp_type = request.GET.getlist("type[]")
    if opt_opp_type == []:
        opt_opp_type = values[7]
    opt_mcu_checked = request.GET.getlist("unique_mcu[]")
    if opt_mcu_checked == []:
        opt_mcu_checked = ["All"]
    opt_sbu_checked = request.GET.getlist("unique_sbu[]")
    if opt_sbu_checked == []:
        opt_sbu_checked = ["All"]
    opt_account_name_checked = request.GET.getlist("unique_account_name[]")
    if opt_account_name_checked == []:
        opt_account_name_checked = ["All"]
    opt_horizontal_checked = request.GET.getlist("unique_horizontal[]")
    if opt_horizontal_checked == []:
        opt_horizontal_checked = ["All"]
    
    sp_cl_stmt = "EXEC [dbo].[get_PipelineReport_Dev] @ptype= %s,@pvertical= %s,@pRegion= %s,@pBU= %s,@pSBU= %s,@pAccount= %s,@pHorizontal= %s,@pOpportunity= %s,@pSalesStage= %s,@pDealSize= %s,@pTier= %s,@ReportDt= %s,@WinzoneID=%s"
    common_args = (
        selected_values["selected_vertical"],
        selected_values["selected_region"],
        selected_values["selected_bu"],
        selected_values["selected_sbu"],
        selected_values["selected_account"],
        'All',
        selected_values["selected_oppType"],
        selected_values["selected_salesstage"],
        selected_values["selected_dealsize"],
        selected_values["selected_tier"],
        selected_values['idx_label_selected'],
        selected_values['selected_winzoneID']
    )

    sp_cl_stmt_dev = "EXEC [dbo].[get_PipelineReport_Dev] @ptype=%s,@pvertical=%s,@pRegion=%s,@pBU=%s,@pSBU=%s,@pAccount=%s,@pHorizontal=%s,@pOpportunity=%s,@pSalesStage=%s,@pDealSize=%s,@pTier=%s,@ReportDt=%s,@WinzoneID=%s"
    common_args_dev = (
        selected_values["selected_vertical"],
        selected_values["selected_region"],
        selected_values["selected_bu"],
        selected_values["selected_sbu"],
        selected_values["selected_account"],
        'All',
        selected_values["selected_oppType"],
        selected_values["selected_salesstage"],
        selected_values["selected_dealsize"],
        selected_values["selected_tier"],
        selected_values['idx_label_selected'],
        ''
        # selected_values['selected_winzoneID']
    )
    common_args_funnel = (
        selected_values["selected_vertical"],
        selected_values["selected_region"],
        selected_values["selected_bu"],
        selected_values["selected_sbu"],
        selected_values["selected_account"],
        'All',
        selected_values["selected_oppType"],
        'All',
        selected_values["selected_dealsize"],
        selected_values["selected_tier"],
        selected_values['idx_label_selected'],
        selected_values['selected_winzoneID']
    )

    # sales = request.GET.getlist("sales_stage[]")
    # salestuple = tuple(sales)
    # salesstr = str(salestuple)
    # deal  = str(tuple(request.GET.getlist("deal_size[]")))
    # vertical = str(tuple(request.GET.getlist("vertical[]")))
    # print(salesstr)
    # print(deal)
    # print(vertical)
    # winzoneQuery = "select distinct [WinZone Opportunity ID] from pipelinedata where [Sales Stage] in %s "
    # winzoneQuery = "select distinct [WinZone Opportunity ID] from pipelinedata where [Sales Stage] in ('3. Solutioning','4. End-Game','5. Negotiation')"
    # # and DealSize in %s and	Vertical in %s
    # print(winzoneQuery)
    # cursor = connection.cursor()
    # cursor.execute(sp_cl_stmt_dev, ("getUnique_Winzone",) + common_args_dev)
    # col_names = [col_desc[0] for col_desc in cursor.description]
    # windf = DataFrame.from_records(cursor.fetchall(), columns=col_names)
    # windata = windf['WinZone Opportunity ID'].values.tolist()
    # print(sp_cl_stmt_dev, ("getUnique_Winzone",) + common_args_dev)
    # ('3. Solutioning','4. End-Game','5. Negotiation') 
    #  ('$2.5m - $10m','$0m - $2.5m')  ('Insurance','Communications Media')

    # cursor = connection.cursor()
    # cursor.execute(sp_cl_stmt_dev, ("FunnelData",) + common_args_funnel)
    # col_names = [col_desc[0] for col_desc in cursor.description]
    # funneldf = DataFrame.from_records(cursor.fetchall(), columns=col_names)
    # funneldf = funneldf.sort_values(by='TCV',ascending=False)
    # # print(funneldf)
    # funnelData = funneldf.to_dict(orient="records")
    try:
        cursor = connection.cursor()
        cursor.execute(sp_cl_stmt, ("getTotal_TCV_Count",) + common_args)
        col_names = [col_desc[0] for col_desc in cursor.description]
        dfTotal = DataFrame.from_records(cursor.fetchall(), columns=col_names)
        dfTotalData = dfTotal.to_dict(orient="records")
        df_count = "{:,.0f}".format(int(dfTotalData[0]['WinZone Opportunity ID']))
        tcv_total = "{:,.1f}".format(dfTotalData[0]['TCV'])
    except:
        dfTotalData=[]
        df_count = 0
        tcv_total = 0
    
    try:
        cursor = connection.cursor()
        cursor.execute(sp_cl_stmt, ("Summary",) + common_args)
        # cursor.execute("[dbo].[get_Pipeline] @ptype=Summary") #sp_call_stmt2,('Base',)+common_args
        col_names = [col_desc[0] for col_desc in cursor.description]
        df = DataFrame.from_records(cursor.fetchall(), columns=col_names)
        dfSS=df
        dfData = dfSS.groupby(["Sales Stage"], as_index=False).agg(
            { "TCV":sum,
            "WinZone Opportunity ID": "count",
            }
        )
        dfData['TCV'] = dfData['TCV'].round(1)
        jsonssData = dfData
        salesData = jsonssData.to_dict(orient="records")
        salesStageData = dfData['Sales Stage'].values.tolist()
        tcv = dfData['TCV'].values.tolist()
        winzoneOpportunityID = dfData['WinZone Opportunity ID'].values.tolist()
        dfDS = df
        dfDSData = dfDS.groupby(["DealSize"], as_index=False).agg(
            { "TCV":sum,
            "WinZone Opportunity ID": "count",
            }
        )
        dfDSData['TCV'] = dfDSData['TCV'].round(1)
        jsondsData = dfDSData
        dealData = jsondsData.to_dict(orient="records")
        dealSizeData = dfDSData['DealSize'].values.tolist()
        dealSizetcv = dfDSData['TCV'].values.tolist()
        dealSizewinzoneOpportunityID = dfDSData['WinZone Opportunity ID'].values.tolist()
        dftype = df
        dfTypeData = dftype.groupby(["R_Deal Type"], as_index=False).agg(
            { "TCV":sum,
            "WinZone Opportunity ID": "count",
            }
        )
        dfTypeData['TCV'] = dfTypeData['TCV'].round(1)
        jsontypeData = dfTypeData
        typeJsonData = jsontypeData.to_dict(orient="records")
        typeData = dfTypeData['R_Deal Type'].values.tolist()
        typetcv = dfTypeData['TCV'].values.tolist()
        typewinzoneOpportunityID = dfTypeData['WinZone Opportunity ID'].values.tolist()

        # dftype = df
        # dfTypeData = dftype.groupby(["Vertical","R_Deal Type"], as_index=False).agg(
        #     { "TCV":sum,
        #     "WinZone Opportunity ID": "count",
        #     }
        # )
        # typeData = dfTypeData['R_Deal Type'].values.tolist()
        # typetcv = dfTypeData['TCV'].values.tolist()
        # typewinzoneOpportunityID = dfTypeData['WinZone Opportunity ID'].values.tolist()

        dfcloseQTR = df
        # dfcloseQTRData = dfcloseQTR.groupby(["Vertical","Close Qtr"], as_index=False).agg(
        #     { "TCV":sum,
        #     "WinZone Opportunity ID": "count",
        #     }
        # )
        closeQtrTypes = ['2022_Q1','2022_Q2','2022_Q3','2022_Q4','2023_Q1','2023_Q2','2023_Q3','2023_Q4','2024_Q1','2024_Q2','2024_Q3','2024_Q4']
        dfcloseQTRData = dfcloseQTR[ dfcloseQTR["CloseQtr"].isin(closeQtrTypes) ].groupby(['CloseQtr'],as_index=False).agg({
            'TCV':lambda x: x.dropna().sum(),
            'WinZone Opportunity ID':lambda x:x.dropna().count()
        })
        dfcloseQTRData['TCV'] = dfcloseQTRData['TCV'].round(1)
        jsoncloseQtrData = dfcloseQTRData
        closeQtrJsonData = jsoncloseQtrData.to_dict(orient="records")
        closeQTRData = dfcloseQTRData['CloseQtr'].values.tolist()
        closeQTRtcv = dfcloseQTRData['TCV'].values.tolist()
        closeQTRwinzoneOpportunityID = dfcloseQTRData['WinZone Opportunity ID'].values.tolist()
    except:
        salesData=[]
        salesStageData=[]
        tcv=[]
        winzoneOpportunityID=[]
        dealData=[]
        dealSizeData=[]
        dealSizetcv=[]
        dealSizewinzoneOpportunityID=[]
        typeJsonData=[]
        typetcv=[]
        typeData=[]
        typewinzoneOpportunityID=[]
        closeQTRData=[]
        closeQTRtcv=[]
        closeQTRwinzoneOpportunityID=[]
    
    return render(request, 'visualize/show_plot.html', context={
        'index_labels':index_labels,
        'winzone_id':selected_values['selected_winzoneID'],
        # 'windata':json.dumps(windata),
        # 'funnelData':json.dumps(funnelData),
        'salesData':json.dumps(salesData),
        'dealData':json.dumps(dealData),
        'typeJsonData':json.dumps(typeJsonData),
        'closeQtrJsonData':json.dumps(closeQtrJsonData),
        'df_count':df_count,
        'tcv_total':tcv_total,
        'idx_label_selected':idx_label_selected,
        "winzoneOpportunityID":json.dumps(winzoneOpportunityID),
        "tcv":json.dumps(tcv),
        "salesStageData":json.dumps(salesStageData),
        "dealSizeData":json.dumps(dealSizeData),
        "dealSizetcv":json.dumps(dealSizetcv),
        "dealSizewinzoneOpportunityID":json.dumps(dealSizewinzoneOpportunityID),
        'plot_label':request.GET.get("plt_label"),
        "typeData":json.dumps(typeData),
        "typetcv":json.dumps(typetcv),
        "typewinzoneOpportunityID":json.dumps(typewinzoneOpportunityID),
        "closeQTRData":json.dumps(closeQTRData),
        "closeQTRtcv":json.dumps(closeQTRtcv),
        "closeQTRwinzoneOpportunityID":json.dumps(closeQTRwinzoneOpportunityID),
        "vertical_unique":values[0],
        "region_unique":values[1],
        "unique_mcu":values[2],
        "unique_sbu":values[3],
        "unique_account_name":values[4],
        "unique_horizontal":values[6],
        "sales_stage":values[8],
        "opp_type":values[7],
        "deal_size":["$0m - $2.5m", "$2.5m - $10m", "$10m - $25m", "$25m - $50m", ">= $50m"],
        "tier_data":values[5],
        "opt_opp_vertical_new":opt_opp_vertical_new,
        "opt_opp_region_new":opt_opp_region_new,
        "opt_sales_stage":opt_sales_stage,
        "opt_deal_size":opt_deal_size,
        "opt_mcu_checked":opt_mcu_checked,
        "opt_sbu_checked":opt_sbu_checked,
        "opt_account_name_checked":opt_account_name_checked,
        "opt_horizontal_checked":opt_horizontal_checked,
        "opt_opp_type":opt_opp_type,
        "opt_tier_data":opt_tier_data,
        "resultDepData":json.dumps(values[9]),
        **selected_values,
        "opt_opp_region_new_filter":json.dumps(opt_opp_region_new),
        "opt_sales_stage_new":json.dumps(opt_sales_stage)
    })

def show_details_table(request):
    # values = getDropDownValues()    
    index_labels = getIndexLabels()
    idx_label_selected = request.GET.get("idx_label")
    if idx_label_selected == None:
        idx_label_selected = index_labels[0]
    values = getDropDownValues(idx_label_selected)
    selected_values = parse_selected_values(request,values[0],values[1],values[7],values[5],index_labels[0])
    # selected_values['idx_label_selected'] = datetime.strptime(selected_values['idx_label_selected'],'%b%d%Y').strftime('%m/%d/%Y')

    opt_opp_vertical_new = request.GET.getlist("vertical[]")
    if opt_opp_vertical_new == []:
        opt_opp_vertical_new = values[0]
    opt_opp_region_new = request.GET.getlist("unq_region[]")
    if opt_opp_region_new == []:
        opt_opp_region_new = values[1]
    stages = request.GET.getlist("sales_stage[]")
    cleaned = [s for s in stages if s != "Duplicate"]
    opt_sales_stage = cleaned
    if opt_sales_stage == []:
        opt_sales_stage = ["3. Solutioning", "4. End-Game", "5. Negotiation"]
    opt_deal_size = request.GET.getlist("deal_size[]")
    if opt_deal_size == []:
            opt_deal_size = ["$0m - $2.5m", "$2.5m - $10m", "$10m - $25m", "$25m - $50m", ">= $50m"]
    opt_tier_data = request.GET.getlist("tier_data[]")
    if opt_tier_data ==[]:
        opt_tier_data = values[5]
    opt_opp_type = request.GET.getlist("type[]")
    if opt_opp_type == []:
        opt_opp_type = values[7]
    opt_mcu_checked = request.GET.getlist("unique_mcu[]")
    if opt_mcu_checked == []:
        opt_mcu_checked = ["All"]
    opt_sbu_checked = request.GET.getlist("unique_sbu[]")
    if opt_sbu_checked == []:
        opt_sbu_checked = ["All"]
    opt_account_name_checked = request.GET.getlist("unique_account_name[]")
    if opt_account_name_checked == []:
        opt_account_name_checked = ["All"]
    opt_horizontal_checked = request.GET.getlist("unique_horizontal[]")
    if opt_horizontal_checked == []:
        opt_horizontal_checked = ["All"]
    
    sp_cl_stmt = "EXEC [dbo].[get_PipelineReport_Dev] @ptype= %s,@pvertical= %s,@pRegion= %s,@pBU= %s,@pSBU= %s,@pAccount= %s,@pHorizontal= %s,@pOpportunity= %s,@pSalesStage= %s,@pDealSize= %s,@pTier= %s,@ReportDt= %s,@WinzoneID=%s"
    common_args = (
        selected_values["selected_vertical"],
        selected_values["selected_region"],
        selected_values["selected_bu"],
        selected_values["selected_sbu"],
        selected_values["selected_account"],
        'All',
        selected_values["selected_oppType"],
        selected_values["selected_salesstage"],
        selected_values["selected_dealsize"],
        selected_values["selected_tier"],
        selected_values['idx_label_selected'],
        selected_values['selected_winzoneID']
    )


    sp_cl_stmt_dev = "EXEC [dbo].[get_PipelineReport_Dev] @ptype=%s,@pvertical=%s,@pRegion=%s,@pBU=%s,@pSBU=%s,@pAccount=%s,@pHorizontal=%s,@pOpportunity=%s,@pSalesStage=%s,@pDealSize=%s,@pTier=%s,@ReportDt=%s,@WinzoneID=%s"
    common_args_dev = (
        selected_values["selected_vertical"],
        selected_values["selected_region"],
        selected_values["selected_bu"],
        selected_values["selected_sbu"],
        selected_values["selected_account"],
        'All',
        selected_values["selected_oppType"],
        selected_values["selected_salesstage"],
        selected_values["selected_dealsize"],
        selected_values["selected_tier"],
        selected_values['idx_label_selected'],
        ''
        # selected_values['selected_winzoneID']
    )

    # cursor = connection.cursor()
    # cursor.execute(sp_cl_stmt_dev, ("getUnique_Winzone",) + common_args_dev)
    # col_names = [col_desc[0] for col_desc in cursor.description]
    # windf = DataFrame.from_records(cursor.fetchall(), columns=col_names)
    # windata = windf['WinZone Opportunity ID'].values.tolist()
    try:
        cursor = connection.cursor()
        cursor.execute(sp_cl_stmt, ("getTotal_TCV_Count",) + common_args)
        col_names = [col_desc[0] for col_desc in cursor.description]
        dfTotal = DataFrame.from_records(cursor.fetchall(), columns=col_names)
        dfTotalData = dfTotal.to_dict(orient="records")
        df_count = "{:,.0f}".format(int(dfTotalData[0]['WinZone Opportunity ID']))
        tcv_total = "{:,.1f}".format(dfTotalData[0]['TCV'])
    except:
        dfTotalData=[]
        df_count=0
        tcv_total=0

    try:
        cursor = connection.cursor()
        # cursor.execute("[dbo].[get_Pipeline] @ptype=Pipelinetbl") #sp_call_stmt2,('Base',)+common_args
        cursor.execute(sp_cl_stmt, ("Pipelinetbl",) + common_args)
        col_names = [col_desc[0] for col_desc in cursor.description]
        df_raw = pd.DataFrame.from_records(cursor.fetchall(),columns=[x[0] for x in cursor.description])
        df_raw.drop(columns=["WinZoneRegion"], inplace=True)
        
        # --- NEW: SORT BY TCV (Highest to Lowest) ---
        if 'TCV' in df_raw.columns:
            # Ensure TCV is numeric for accurate sorting, then sort descending
            df_raw['TCV'] = pd.to_numeric(df_raw['TCV'], errors='coerce')
            df_raw = df_raw.sort_values(by='TCV', ascending=False)
        # --------------------------------------------
            
        df_raw = df_raw.fillna('-')
        col_header = df_raw.columns.tolist()
        df_raw['Created Date'] =pd.to_datetime(df_raw['Created Date']).dt.strftime('%d-%m-%Y')
        df_raw['CloseDate'] =pd.to_datetime(df_raw['CloseDate']).dt.strftime('%d-%m-%Y')
        tableData = df_raw.to_dict(orient="records")
        html_code = ""
    except:
        tableData=[]
        html_code = ""
        col_header=[]
    # html_code = html_code + df_raw.to_html(sparsify=False, index=False, classes=["table-hover", "table-condensed","table","table-bordered"])
    # html_code = html_code + "<br>"
    return render(request, 'visualize/pipeline_details_table.html',context={
        'plot_label':"Pipeline Table View",
        'winzone_id':selected_values['selected_winzoneID'],
        # 'windata':json.dumps(windata),
        'df_count':df_count,
        'tcv_total':tcv_total,
        'index_labels':index_labels,
        'idx_label_selected':idx_label_selected,
        "html_data_pivot":html_code,
        "vertical_unique":values[0],
        "region_unique":values[1],
        "unique_mcu":values[2],
        "unique_sbu":values[3],
        "unique_account_name":values[4],
        "unique_horizontal":values[6],
        "sales_stage":values[8],
        "opp_type":values[7],
        "deal_size":["$0m - $2.5m", "$2.5m - $10m", "$10m - $25m", "$25m - $50m", ">= $50m"],
        "tier_data":values[5],
        "opt_opp_vertical_new":opt_opp_vertical_new,
        "opt_opp_region_new":opt_opp_region_new,
        "opt_sales_stage":opt_sales_stage,
        "opt_deal_size":opt_deal_size,
        "opt_mcu_checked":opt_mcu_checked,
        "opt_sbu_checked":opt_sbu_checked,
        "opt_account_name_checked":opt_account_name_checked,
        "opt_horizontal_checked":opt_horizontal_checked,
        "opt_opp_type":opt_opp_type,
        "opt_tier_data":opt_tier_data,
        "tableData":json.dumps(tableData),
        "col_header":json.dumps(col_header),
        "resultDepData":json.dumps(values[9]),
        **selected_values,
        "opt_opp_region_new_filter":json.dumps(opt_opp_region_new)
    })
def show_pipeline_details_plot(index,label,request):
    # values = getDropDownValues()
    index_labels = getIndexLabels()
    idx_label_selected = request.GET.get("idx_label")
    if idx_label_selected == None:
        idx_label_selected = index_labels[0]
    values = getDropDownValues(idx_label_selected)
    selected_values = parse_selected_values(request,values[0],values[1],values[7],values[5],index_labels[0])
    # selected_values['idx_label_selected'] = datetime.strptime(selected_values['idx_label_selected'],'%b%d%Y').strftime('%m/%d/%Y')

    opt_opp_vertical_new = request.GET.getlist("vertical[]")
    if opt_opp_vertical_new == []:
        opt_opp_vertical_new = values[0]
    opt_opp_region_new = request.GET.getlist("unq_region[]")
    if opt_opp_region_new == []:
        opt_opp_region_new = values[1]
    
    stages = request.GET.getlist("sales_stage[]")
    cleaned = [s for s in stages if s != "Duplicate"]
    opt_sales_stage = cleaned
    if opt_sales_stage == []:
        opt_sales_stage = ["3. Solutioning", "4. End-Game", "5. Negotiation"]
    opt_deal_size = request.GET.getlist("deal_size[]")
    if opt_deal_size == []:
            opt_deal_size = ["$0m - $2.5m", "$2.5m - $10m", "$10m - $25m", "$25m - $50m", ">= $50m"]
    opt_tier_data = request.GET.getlist("tier_data[]")
    if opt_tier_data ==[]:
        opt_tier_data = values[5]
    opt_opp_type = request.GET.getlist("type[]")
    if opt_opp_type == []:
        opt_opp_type = values[7]
    opt_mcu_checked = request.GET.getlist("unique_mcu[]")
    if opt_mcu_checked == []:
        opt_mcu_checked = ["All"]
    opt_sbu_checked = request.GET.getlist("unique_sbu[]")
    if opt_sbu_checked == []:
        opt_sbu_checked = ["All"]
    opt_account_name_checked = request.GET.getlist("unique_account_name[]")
    if opt_account_name_checked == []:
        opt_account_name_checked = ["All"]
    opt_horizontal_checked = request.GET.getlist("unique_horizontal[]")
    if opt_horizontal_checked == []:
        opt_horizontal_checked = ["All"]
    
    sp_cl_stmt = "EXEC [dbo].[get_PipelineReport_Dev] @ptype= %s,@pvertical= %s,@pRegion= %s,@pBU= %s,@pSBU= %s,@pAccount= %s,@pHorizontal= %s,@pOpportunity= %s,@pSalesStage= %s,@pDealSize= %s,@pTier= %s,@ReportDt= %s,@WinzoneID=%s"
    common_args = (
        selected_values["selected_vertical"],
        selected_values["selected_region"],
        selected_values["selected_bu"],
        selected_values["selected_sbu"],
        selected_values["selected_account"],
        'All',
        selected_values["selected_oppType"],
        selected_values["selected_salesstage"],
        selected_values["selected_dealsize"],
        selected_values["selected_tier"],
        selected_values['idx_label_selected'],
        selected_values['selected_winzoneID']
    )
    sp_cl_stmt_dev = "EXEC [dbo].[get_PipelineReport_Dev] @ptype=%s,@pvertical=%s,@pRegion=%s,@pBU=%s,@pSBU=%s,@pAccount=%s,@pHorizontal=%s,@pOpportunity=%s,@pSalesStage=%s,@pDealSize=%s,@pTier=%s,@ReportDt=%s,@WinzoneID=%s"
    common_args_dev = (
        selected_values["selected_vertical"],
        selected_values["selected_region"],
        selected_values["selected_bu"],
        selected_values["selected_sbu"],
        selected_values["selected_account"],
        'All',
        selected_values["selected_oppType"],
        selected_values["selected_salesstage"],
        selected_values["selected_dealsize"],
        selected_values["selected_tier"],
        selected_values['idx_label_selected'],
        ''
        # selected_values['selected_winzoneID']
    )
    # try:
    #     cursor = connection.cursor()
    #     cursor.execute(sp_cl_stmt_dev, ("getUnique_Winzone",) + common_args_dev)
    #     col_names = [col_desc[0] for col_desc in cursor.description]
    #     windf = DataFrame.from_records(cursor.fetchall(), columns=col_names)
    #     windata = windf['WinZone Opportunity ID'].values.tolist()
    # except:
    #     windata=[]

    try:
        cursor = connection.cursor()
        cursor.execute(sp_cl_stmt, ("getTotal_TCV_Count",) + common_args)
        col_names = [col_desc[0] for col_desc in cursor.description]
        dfTotal = DataFrame.from_records(cursor.fetchall(), columns=col_names)
        dfTotalData = dfTotal.to_dict(orient="records")
        df_count = "{:,.0f}".format(int(dfTotalData[0]['WinZone Opportunity ID']))
        tcv_total = "{:,.1f}".format(dfTotalData[0]['TCV'])
    except:
        dfTotalData=[]
        df_count=0
        tcv_total=0

    try:
        cursor.execute(sp_cl_stmt, ("details",) + common_args)
        # cursor.execute("select [WinZone Opportunity ID],R_Vertical Vertical,[Account Name],[Opportunity Record Type Name],[Sales Stage],round([Gross TCV $]/1000000,2) as TCV,DealSize,CloseMonth [Close Date] from pipelinedata where [Sales Stage] IN('1. Engagement','2. Shaping','3. Solutioning','4. End-Game','5. Negotiation')") #sp_call_stmt2,('Base',)+common_args
        col_names = [col_desc[0] for col_desc in cursor.description]
        df = DataFrame.from_records(cursor.fetchall(), columns=col_names)
        col_header = df.columns.tolist()
        tableData =df.to_dict(orient="records")
    except:
        tableData=[]
        col_header=[]
    return render(request, 'visualize/show_pipeline_details.html', context={
        'plot_label':request.GET.get("plt_label"),
        'winzone_id':selected_values['selected_winzoneID'],
        # 'windata':json.dumps(windata),
        'df_count':df_count,
        'tcv_total':tcv_total,
        'index_labels':index_labels,
        'idx_label_selected':idx_label_selected,
        'tableData':json.dumps(tableData),
        'col_header':json.dumps(col_header),
        "vertical_unique":values[0],
        "region_unique":values[1],
        "unique_mcu":values[2],
        "unique_sbu":values[3],
        "unique_account_name":values[4],
        "unique_horizontal":values[6],
        "sales_stage":values[8],
        "opp_type":values[7],
        "deal_size":["$0m - $2.5m", "$2.5m - $10m", "$10m - $25m", "$25m - $50m", ">= $50m"],
        "tier_data":values[5],
        "opt_opp_vertical_new":opt_opp_vertical_new,
        "opt_opp_region_new":opt_opp_region_new,
        "opt_sales_stage":opt_sales_stage,
        "opt_deal_size":opt_deal_size,
        "opt_mcu_checked":opt_mcu_checked,
        "opt_sbu_checked":opt_sbu_checked,
        "opt_account_name_checked":opt_account_name_checked,
        "opt_horizontal_checked":opt_horizontal_checked,
        "opt_opp_type":opt_opp_type,
        "opt_tier_data":opt_tier_data,
        "resultDepData":json.dumps(values[9]),
        **selected_values,
        "opt_opp_region_new_filter":json.dumps(opt_opp_region_new)
    })
def show_pipeline_details_SBU_plot(index, label, request):
    # values = getDropDownValues()
    index_labels = getIndexLabels()
    idx_label_selected = request.GET.get("idx_label")
    if idx_label_selected == None:
        idx_label_selected = index_labels[0]
    values = getDropDownValues(idx_label_selected)
    selected_values = parse_selected_values(request,values[0],values[1],values[7],values[5],index_labels[0])
    # selected_values['idx_label_selected'] = datetime.strptime(selected_values['idx_label_selected'],'%b%d%Y').strftime('%m/%d/%Y')

    opt_opp_vertical_new = request.GET.getlist("vertical[]")
    if opt_opp_vertical_new == []:
        opt_opp_vertical_new = values[0]
    opt_opp_region_new = request.GET.getlist("unq_region[]")
    if opt_opp_region_new == []:
        opt_opp_region_new = values[1]
    
    stages = request.GET.getlist("sales_stage[]")
    cleaned = [s for s in stages if s != "Duplicate"]
    opt_sales_stage = cleaned
    if opt_sales_stage == []:
        opt_sales_stage = ["3. Solutioning", "4. End-Game", "5. Negotiation"]
    opt_deal_size = request.GET.getlist("deal_size[]")
    if opt_deal_size == []:
            opt_deal_size = ["$0m - $2.5m", "$2.5m - $10m", "$10m - $25m", "$25m - $50m", ">= $50m"]
    opt_tier_data = request.GET.getlist("tier_data[]")
    if opt_tier_data ==[]:
        opt_tier_data = values[5]
    opt_opp_type = request.GET.getlist("type[]")
    if opt_opp_type == []:
        opt_opp_type = values[7]
    opt_mcu_checked = request.GET.getlist("unique_mcu[]")
    if opt_mcu_checked == []:
        opt_mcu_checked = ["All"]
    opt_sbu_checked = request.GET.getlist("unique_sbu[]")
    if opt_sbu_checked == []:
        opt_sbu_checked = ["All"]
    opt_account_name_checked = request.GET.getlist("unique_account_name[]")
    if opt_account_name_checked == []:
        opt_account_name_checked = ["All"]
    opt_horizontal_checked = request.GET.getlist("unique_horizontal[]")
    if opt_horizontal_checked == []:
        opt_horizontal_checked = ["All"]
    
    sp_cl_stmt = "EXEC [dbo].[get_PipelineReport_Dev] @ptype= %s,@pvertical= %s,@pRegion= %s,@pBU= %s,@pSBU= %s,@pAccount= %s,@pHorizontal= %s,@pOpportunity= %s,@pSalesStage= %s,@pDealSize= %s,@pTier= %s,@ReportDt= %s,@WinzoneID=%s"
    common_args = (
        selected_values["selected_vertical"],
        selected_values["selected_region"],
        selected_values["selected_bu"],
        selected_values["selected_sbu"],
        selected_values["selected_account"],
        'All',
        selected_values["selected_oppType"],
        selected_values["selected_salesstage"],
        selected_values["selected_dealsize"],
        selected_values["selected_tier"],
        selected_values['idx_label_selected'],
        selected_values['selected_winzoneID']
    )

    sp_cl_stmt_dev = "EXEC [dbo].[get_PipelineReport_Dev] @ptype=%s,@pvertical=%s,@pRegion=%s,@pBU=%s,@pSBU=%s,@pAccount=%s,@pHorizontal=%s,@pOpportunity=%s,@pSalesStage=%s,@pDealSize=%s,@pTier=%s,@ReportDt=%s,@WinzoneID=%s"
    common_args_dev = (
        selected_values["selected_vertical"],
        selected_values["selected_region"],
        selected_values["selected_bu"],
        selected_values["selected_sbu"],
        selected_values["selected_account"],
        'All',
        selected_values["selected_oppType"],
        selected_values["selected_salesstage"],
        selected_values["selected_dealsize"],
        selected_values["selected_tier"],
        selected_values['idx_label_selected'],
        ''
        # selected_values['selected_winzoneID']
    )
    # cursor = connection.cursor()
    # cursor.execute(sp_cl_stmt_dev, ("getUnique_Winzone",) + common_args_dev)
    # col_names = [col_desc[0] for col_desc in cursor.description]
    # windf = DataFrame.from_records(cursor.fetchall(), columns=col_names)
    # windata = windf['WinZone Opportunity ID'].values.tolist()

    try:
        cursor = connection.cursor()
        cursor.execute(sp_cl_stmt, ("getTotal_TCV_Count",) + common_args)
        col_names = [col_desc[0] for col_desc in cursor.description]
        dfTotal = DataFrame.from_records(cursor.fetchall(), columns=col_names)
        dfTotalData = dfTotal.to_dict(orient="records")
        df_count = "{:,.0f}".format(int(dfTotalData[0]['WinZone Opportunity ID']))
        tcv_total = "{:,.1f}".format(dfTotalData[0]['TCV'])
    except:
        dfTotalData=[]
        df_count=0
        tcv_total=0

    try:
        cursor.execute(sp_cl_stmt, ("sbu_by_salesstage",) + common_args)
        # cursor.execute("select [WinZone Opportunity ID],R_Vertical Vertical,[Account Name],[Opportunity Record Type Name],[Sales Stage],DealSize,round([Gross TCV $]/1000000,2) as TCV,SBU1 from pipelinedata where [Sales Stage] IN('1. Engagement','2. Shaping','3. Solutioning','4. End-Game','5. Negotiation')") #sp_call_stmt2,('Base',)+common_args
        col_names = [col_desc[0] for col_desc in cursor.description]
        df = DataFrame.from_records(cursor.fetchall(), columns=col_names)
        tableData = df.to_dict(orient="records")
    except:
        tableData=[]
    try:
        cursor.execute(sp_cl_stmt, ("sbu_by_closeqtr",) + common_args)
        # cursor.execute("select R_Vertical Vertical,[R_Market Unit] Region,CloseQtr,SBU1,count([WinZone Opportunity ID]) [WinZone Opportunity ID],round(sum(isnull([Gross TCV $],0))/1000000,1) TCV from pipelinedata where [Sales Stage] IN('1. Engagement','2. Shaping','3. Solutioning','4. End-Game','5. Negotiation') group by R_Vertical,[R_Market Unit],CloseQtr,SBU1") #sp_call_stmt2,('Base',)+common_args
        col_names = [col_desc[0] for col_desc in cursor.description]
        df2 = DataFrame.from_records(cursor.fetchall(), columns=col_names)
        tableData2 = df2.to_dict(orient="records")
    except:
        tableData2=[]
    return render(request, 'visualize/show_pipeline_details_SBU.html', context={
        'plot_label':request.GET.get("plt_label"),
        'winzone_id':selected_values['selected_winzoneID'],
        # 'windata':json.dumps(windata),
        'df_count':df_count,
        'tcv_total':tcv_total,
        'index_labels':index_labels,
        'idx_label_selected':idx_label_selected,
        'tableData':json.dumps(tableData),
        'tableData2':json.dumps(tableData2),
        "vertical_unique":values[0],
        "region_unique":values[1],
        "unique_mcu":values[2],
        "unique_sbu":values[3],
        "unique_account_name":values[4],
        "unique_horizontal":values[6],
        "sales_stage":values[8],
        "opp_type":values[7],
        "deal_size":["$0m - $2.5m", "$2.5m - $10m", "$10m - $25m", "$25m - $50m", ">= $50m"],
        "tier_data":values[5],
        "opt_opp_vertical_new":opt_opp_vertical_new,
        "opt_opp_region_new":opt_opp_region_new,
        "opt_sales_stage":opt_sales_stage,
        "opt_deal_size":opt_deal_size,
        "opt_mcu_checked":opt_mcu_checked,
        "opt_sbu_checked":opt_sbu_checked,
        "opt_account_name_checked":opt_account_name_checked,
        "opt_horizontal_checked":opt_horizontal_checked,
        "opt_opp_type":opt_opp_type,
        "opt_tier_data":opt_tier_data,
        "resultDepData":json.dumps(values[9]),
        **selected_values,
        "opt_opp_region_new_filter":json.dumps(opt_opp_region_new)
    })
def show_movement(request):
    # values = getDropDownValues()
    index_labels = getIndexLabels()
    idx_label_selected_A = request.GET.get("idx_label_A")
    if idx_label_selected_A == None:
        idx_label_selected_A = index_labels[0]
    values = getDropDownValues(idx_label_selected_A)
    
    idx_label_selected_B = request.GET.get("idx_label_B")
    if idx_label_selected_B == None:
        idx_label_selected_B = index_labels[0]


    selected_values = parse_selected_values(request,values[0],values[1],values[7],values[5],index_labels[0])
    # selected_values['idx_label_selected'] = datetime.strptime(selected_values['idx_label_selected'],'%b%d%Y').strftime('%m/%d/%Y')

    opt_opp_vertical_new = request.GET.getlist("vertical[]")
    if opt_opp_vertical_new == []:
        opt_opp_vertical_new = values[0]
    opt_opp_region_new = request.GET.getlist("unq_region[]")
    if opt_opp_region_new == []:
        opt_opp_region_new = values[1]
    
    stages = request.GET.getlist("sales_stage[]")
    cleaned = [s for s in stages if s != "Duplicate"]
    opt_sales_stage = cleaned
    if opt_sales_stage == []:
        opt_sales_stage = ["3. Solutioning", "4. End-Game", "5. Negotiation"]
    opt_deal_size = request.GET.getlist("deal_size[]")
    if opt_deal_size == []:
            opt_deal_size = ["$0m - $2.5m", "$2.5m - $10m", "$10m - $25m", "$25m - $50m", ">= $50m"]
    opt_tier_data = request.GET.getlist("tier_data[]")
    if opt_tier_data ==[]:
        opt_tier_data = values[5]
    opt_opp_type = request.GET.getlist("type[]")
    if opt_opp_type == []:
        opt_opp_type = values[7]
    opt_mcu_checked = request.GET.getlist("unique_mcu[]")
    if opt_mcu_checked == []:
        opt_mcu_checked = ["All"]
    opt_sbu_checked = request.GET.getlist("unique_sbu[]")
    if opt_sbu_checked == []:
        opt_sbu_checked = ["All"]
    opt_account_name_checked = request.GET.getlist("unique_account_name[]")
    if opt_account_name_checked == []:
        opt_account_name_checked = ["All"]
    opt_horizontal_checked = request.GET.getlist("unique_horizontal[]")
    if opt_horizontal_checked == []:
        opt_horizontal_checked = ["All"]
    
    sp_cl_stmt = "EXEC [dbo].[get_PipelineReport_Dev] @ptype= %s,@pvertical= %s,@pRegion= %s,@pBU= %s,@pSBU= %s,@pAccount= %s,@pHorizontal= %s,@pOpportunity= %s,@pSalesStage= %s,@pDealSize= %s,@pTier= %s,@ReportDt= %s,@WinzoneID=%s"
    common_args1 = (
        selected_values["selected_vertical"],
        selected_values["selected_region"],
        selected_values["selected_bu"],
        selected_values["selected_sbu"],
        selected_values["selected_account"],
        'All',
        selected_values["selected_oppType"],
        '1. Engagement,2. Shaping,3. Solutioning,4. End-Game,5. Negotiation',
        selected_values["selected_dealsize"],
        selected_values["selected_tier"],
        # selected_values['idx_label_selected_B'],
        idx_label_selected_B,
        # datetime.strptime(idx_label_selected_B,'%b%d%Y').strftime('%m/%d/%Y'),
        selected_values['selected_winzoneID']
    )
    common_args2 = (
        selected_values["selected_vertical"],
        selected_values["selected_region"],
        selected_values["selected_bu"],
        selected_values["selected_sbu"],
        selected_values["selected_account"],
        'All',
        selected_values["selected_oppType"],
        'Client Withdraw,Cognizant Withdraw,Won,Lost',
        selected_values["selected_dealsize"],
        selected_values["selected_tier"],
        # selected_values['idx_label_selected_B'],
        idx_label_selected_B,
        # datetime.strptime(idx_label_selected_B,'%b%d%Y').strftime('%m/%d/%Y'),
        selected_values['selected_winzoneID']
    )
    common_args3 = (
        selected_values["selected_vertical"],
        selected_values["selected_region"],
        selected_values["selected_bu"],
        selected_values["selected_sbu"],
        selected_values["selected_account"],
        'All',
        selected_values["selected_oppType"],
        'Won',
        selected_values["selected_dealsize"],
        selected_values["selected_tier"],
        # selected_values['idx_label_selected_B'],
        idx_label_selected_B,
        # datetime.strptime(idx_label_selected_B,'%b%d%Y').strftime('%m/%d/%Y'),
        selected_values['selected_winzoneID']
    )
    common_argsA = (
        selected_values["selected_vertical"],
        selected_values["selected_region"],
        selected_values["selected_bu"],
        selected_values["selected_sbu"],
        selected_values["selected_account"],
        'All',
        selected_values["selected_oppType"],
        selected_values["selected_salesstage"],
        selected_values["selected_dealsize"],
        selected_values["selected_tier"],
        # selected_values['idx_label_selected_A'],
        idx_label_selected_A,
        # datetime.strptime(idx_label_selected_A,'%b%d%Y').strftime('%m/%d/%Y'),
        selected_values['selected_winzoneID']
    )
    common_argsB = (
        selected_values["selected_vertical"],
        selected_values["selected_region"],
        selected_values["selected_bu"],
        selected_values["selected_sbu"],
        selected_values["selected_account"],
        'All',
        selected_values["selected_oppType"],
        selected_values["selected_salesstage"],
        selected_values["selected_dealsize"],
        selected_values["selected_tier"],
        # selected_values['idx_label_selected_B'],
        idx_label_selected_B,
        # datetime.strptime(idx_label_selected_B,'%b%d%Y').strftime('%m/%d/%Y'),
        selected_values['selected_winzoneID']
    )

    sp_cl_stmt_dev = "EXEC [dbo].[get_PipelineReport_Dev] @ptype=%s,@pvertical=%s,@pRegion=%s,@pBU=%s,@pSBU=%s,@pAccount=%s,@pHorizontal=%s,@pOpportunity=%s,@pSalesStage=%s,@pDealSize=%s,@pTier=%s,@ReportDt=%s,@WinzoneID=%s"
    common_args_dev = (
        selected_values["selected_vertical"],
        selected_values["selected_region"],
        selected_values["selected_bu"],
        selected_values["selected_sbu"],
        selected_values["selected_account"],
        'All',
        selected_values["selected_oppType"],
        selected_values["selected_salesstage"],
        selected_values["selected_dealsize"],
        selected_values["selected_tier"],
        selected_values['idx_label_selected'],
        ''
        # selected_values['selected_winzoneID']
    )
    # cursor = connection.cursor()
    # cursor.execute(sp_cl_stmt_dev, ("getUnique_Winzone",) + common_args_dev)
    # col_names = [col_desc[0] for col_desc in cursor.description]
    # windf = DataFrame.from_records(cursor.fetchall(), columns=col_names)
    # windata = windf['WinZone Opportunity ID'].values.tolist()


    html_code = ""
    plot_div = ""
    plot_div_2 = ""
    plot_div_3 = ""
    plot_div_4 = ""
    plot_div_5 = ""
    plot_div_6 = ""
    plot_div_change = ""
    plot_div_pipe = ""

    # stages = {"1. Engagement", "2. Shaping", "3. Solutioning", "4. End-Game", "5. Negotiation"}
    showExtraTable = False
    dealsData = []
    summaryData = []
    totalData = []
    movedData = []
    changeData = []
    tableData = []
    tableColumns = []
    if ( (idx_label_selected_A != idx_label_selected_B)): 
        cursor = connection.cursor()
        cursor.execute(sp_cl_stmt, ("PipelineData_A",) + common_argsA)
        # cursor.execute("select [R_Market Unit] Region,[Account Name],[Opportunity Name],round([Gross TCV $]/1000000,2) TCV,[Sales Stage],[WinZone Opportunity ID],R_Vertical Vertical,DealSize,CloseDate,[Created Date] from pipelinedata where [Sales Stage] IN('1. Engagement','2. Shaping','3. Solutioning','4. End-Game','5. Negotiation','Client Withdraw','Cognizant Withdraw','Won','Lost') and uploadedon = '05/06/2024'")
        col_names = [col_desc[0] for col_desc in cursor.description]
        new_df_raw = DataFrame.from_records(cursor.fetchall(), columns=col_names)
        df_raw = new_df_raw
        cursor.execute(sp_cl_stmt, ("PipelineData_A",) + common_argsB)
        # cursor.execute("select [R_Market Unit] Region,[Account Name],[Opportunity Name],round([Gross TCV $]/1000000,2) TCV,[Sales Stage],[WinZone Opportunity ID],R_Vertical Vertical,DealSize,CloseDate,[Created Date] from pipelinedata where [Sales Stage] IN('1. Engagement','2. Shaping','3. Solutioning','4. End-Game','5. Negotiation','Client Withdraw','Cognizant Withdraw','Won','Lost') and uploadedon = '05/15/2024'")
        col_names = [col_desc[0] for col_desc in cursor.description]
        new_df_raw_B = DataFrame.from_records(cursor.fetchall(), columns=col_names)
        df_raw_B = new_df_raw_B
        if df_raw.empty and df_raw_B.empty:
            html_code = ""
            plot_div = ""
            plot_div_2 = ""
            plot_div_3 = ""
            plot_div_4 = ""
            plot_div_5 = ""
            plot_div_6 = ""
            plot_div_change = ""
            plot_div_pipe = ""

            # stages = {"1. Engagement", "2. Shaping", "3. Solutioning", "4. End-Game", "5. Negotiation"}
            showExtraTable = False
            dealsData = []
            summaryData = []
            totalData = []
            movedData = []
            changeData = []
            tableData = []
            tableColumns = []
        else:
            df_raw_B["CloseDate"] = pd.to_datetime(df_raw_B["CloseDate"])
            df_raw_B["Created Date"] = pd.to_datetime(df_raw_B["Created Date"])
            showExtraTable = True
            df_A = df_raw
            # [(df_raw["Sales Stage"].isin(stages)) & df_raw["Region"].isin(region_list) & df_raw["Vertical"].isin(bu_list)]
            if not df_A.empty:
                df_A.loc[:,"Moved To Stage"] = "" #29
                df_A.loc[:,"Stage Change"] = "" #30
            else:
                df_A["Moved To Stage"] = ""
                df_A["Stage Change"] = ""
            df_A_list = df_A.values.tolist()
            for i in df_A_list:
                winZoneID = i[5]
                db_record = df_raw_B[df_raw_B["WinZone Opportunity ID"] == float(winZoneID)]
                # print(db_record)
                if len(db_record.index) == 0:
                    i[10] = "Missing"
                else:
                    i[10] = db_record["Sales Stage"].values[0]
                if i[10] == i[4]:
                    i[11] = "No"
                else:
                    i[11] = "Yes"
                    
            # df_A = pd.DataFrame( df_A_list, columns=["Region", "BU", "SBU1", "Account Name", "Opportunity Name", "Practice Area for Strategic Deals","TCV", "Deal Duration (Months)", "Win Probability (%)", "Pursuit Lead: Full Name", "Sales Stage", "WinZone Opportunity ID", "Strategic", "Competitors", "Vertical", "Deal Size", "Close Date", "Cre Date", "Close Qtr", "Type", "Biz Unit", "MCU", "SBU", "Close Year", "Cycle Time", "Horizontal", "Region1", "SubRegion", "Load Date", "Moved To Stage", "Stage Change"  ]) 
            df_A = pd.DataFrame( df_A_list, columns=["Region", "Account Name", "Opportunity Name", "TCV", "Sales Stage", "WinZone Opportunity ID", "Vertical", "Deal Size", "CloseDate", "Created Date","Moved To Stage", "Stage Change"  ])
            # print(df_A)
            if not df_A.empty:
                df = pd.pivot_table(df_A,values="TCV",index=["Sales Stage"],columns=["Moved To Stage"], aggfunc=numpy.sum, fill_value=0, margins=True, margins_name="Totals")
            else:
                df = pd.DataFrame()
            
            df_2 = df_A.groupby(["Sales Stage","Moved To Stage"], as_index=False).agg(
            
                { "TCV":sum,
                }
            )

            df_2.sort_values(by=['Sales Stage'],axis=0,ascending=True,inplace = True)

            
            df_2_2 = df_A[df_A["Stage Change"].isin({"Yes"})].groupby(["Moved To Stage"], as_index=False).agg(
            
                { "TCV":sum,
                "WinZone Opportunity ID":"count"
                }
            )
            df_2_2 = df_2_2.rename(columns={"WinZone Opportunity ID":"Deals"})

            df_change = df_A.groupby(["Stage Change"], as_index=False).agg(
            
                { "TCV":sum,
                
                }
            )

            df_change_sum = df_change.agg( {"TCV":sum} )
            pipeline_A = df_change_sum["TCV"]
            changeData = df_change.to_dict(orient="records")
            datam = df.iloc[:,0]
            df_dum = df
            df_dum = df_dum.rename(columns={"3. Solutioning": "Solutioning",
                                            "4. End-Game":"End-Game",
                                            "5. Negotiation":"Negotiation",
                                            "1. Engagement":"Engagement",
                                            "2. Shaping":"Shaping"})
            tableColumns = df_dum.columns.tolist()
            tableData = df_dum.to_dict(orient="records")
            html_code = "<br>"
            html_code = html_code + df.to_html(sparsify=False, index=True, classes=["table-hover", "table-condensed","table","table-bordered"])
            #html_code = html_code + df.to_html(sparsify=False, index=True)
            html_code = html_code + "<br>"   

            plt = make_subplots(rows=1, cols=1, specs=[[{'type':'domain'}]],subplot_titles=[''])
            plt.add_trace(go_2.Pie(labels=df_change["Stage Change"], values=df_change["TCV"], insidetextorientation='radial', hole=0.3, name=""),1,1)
            plt.update_traces(textinfo="label+value", texttemplate='%{value:$,.1f}')
            plt.update_layout(template='plotly_dark', autosize=False, width=300,height=500, title="Change in A w.r.t B")
            plot_div_change = plot(plt,output_type='div', show_link=False)

            movedData = df_2_2.to_dict(orient="records")
            plt = make_subplots(specs=[[{"secondary_y": True}]])
            plt.add_trace(go_2.Bar(x=df_2_2["Moved To Stage"], y=df_2_2["TCV"] , name="by TCV", marker_color='lightsalmon'), secondary_y=False)
            plt.update_traces(texttemplate='%{value:$,.1f}',textposition="outside")
            plt.update_layout(title="Moved Stages between A and B")
            plt.update_layout(template='plotly_dark', autosize=False, width=1180,height=500)
            plt.add_trace(go_2.Scatter(x=df_2_2["Moved To Stage"], y=df_2_2["Deals"], mode="markers+lines" , name="by #Deals",
                marker=dict(size=12, line=dict(width=2,color='DarkSlateGrey')), line=dict(dash='dash') ) , secondary_y=True)
            plt.update_yaxes(showgrid=False)
            plot_div = plot(plt,output_type='div', show_link=False)


            
            #Pie Plots

            p1 = df_2[df_2["Sales Stage"].isin({"1. Engagement"})]
            p2 = df_2[df_2["Sales Stage"].isin({"2. Shaping"})]
            p3 = df_2[df_2["Sales Stage"].isin({"3. Solutioning"})]
            p4 = df_2[df_2["Sales Stage"].isin({"4. End-Game"})]
            p5 = df_2[df_2["Sales Stage"].isin({"5. Negotiation"})]

        # ////////////////////////////////////////////////////////////////////
            plt = make_subplots(rows=1, cols=5, specs=[[{'type':'domain'}, {'type':'domain'}, {'type':'domain'}, {'type':'domain'}, {'type':'domain'}]],subplot_titles=['Engagement', 'Shaping', 'Solutioning','End Game','Negotiation'])
            plt.add_trace(go_2.Pie(labels=p1["Moved To Stage"], values=p1["TCV"], insidetextorientation='radial', hole=0.3, name="Engagement"),1,1)
            plt.add_trace(go_2.Pie(labels=p2["Moved To Stage"], values=p2["TCV"], insidetextorientation='radial', hole=0.3, name="Shaping"),1,2)
            plt.add_trace(go_2.Pie(labels=p3["Moved To Stage"], values=p3["TCV"], insidetextorientation='radial', hole=0.3, name="Solutioning"),1,3)
            plt.add_trace(go_2.Pie(labels=p4["Moved To Stage"], values=p4["TCV"], insidetextorientation='radial', hole=0.3, name="End Game"),1,4)
            plt.add_trace(go_2.Pie(labels=p5["Moved To Stage"], values=p5["TCV"], insidetextorientation='radial', hole=0.3, name="Negotiation"),1,5)
                
            plt.update_traces(textinfo="label+value", texttemplate='%{value:$,.1f}')
            plt.update_layout(template='plotly_dark', autosize=False, width=1800,height=500)
            plt.update_layout(title="Stage level movements")
            plot_div_2 = plot(plt,output_type='div', show_link=False)

            df_B = df_raw_B
            # [ df_raw_B["Region"].isin(region_list) & df_raw_B["Vertical"].isin(bu_list)]

            df_pipeline_B = df_B
            df_pipeline_B = df_pipeline_B.agg( {"TCV":sum})
            pipeline_B = df_pipeline_B["TCV"]
            # print("Pipeline value of B:")
            
            data = {'Pipeline':['A','B'],'TCV':[pipeline_A,pipeline_B]}
            pipe_A_B = pd.DataFrame(data, columns=['Pipeline','TCV'])
            totalData = pipe_A_B.to_dict(orient="records")

            plt = go.bar(pipe_A_B, x="Pipeline", y='TCV', orientation='v',template='plotly_dark', barmode="group",text='TCV')
            plt.update_traces(texttemplate='%{text:.2s}')
            plt.update_layout(title="Pipeline Totals", autosize=False, width=300,height=500)
            plot_div_pipe = plot(plt,output_type='div', show_link=False)    
            if not df_B.empty:
                df_B.loc[:,"Opp Presence"] = "" #29
            else:
                df_B["Opp Presence"] = ""
            df_B_list = df_B.values.tolist()
            
            for j in df_B_list:
                    winZoneID = j[5]
                    db_record = df_raw[df_raw["WinZone Opportunity ID"] == float(winZoneID)]
                    
                    if len(db_record.index) == 0:
                        j[10] = "New"
                    else:
                        j[10] ="Existing"
            df_B = pd.DataFrame( df_B_list, columns=["Region", "Account Name", "Opportunity Name", "TCV",  "Sales Stage", "WinZone Opportunity ID", "Vertical", "Deal Size", "CloseDate", "Created Date", "Opp Presence" ]) 
            if not df_B.empty:
                df_B.loc[:,"CloseDate"] = pd.to_datetime(df_B["CloseDate"])
                df_B.loc[:,"Created Date"] = pd.to_datetime(df_B["Created Date"])
            

            df_B = df_B[df_B["Opp Presence"].isin({"New"})]

            df_B_2 = df_B.groupby(["Sales Stage","Deal Size"], as_index=False).agg(
                { "TCV":sum,
                }
            )

            summaryData = df_B_2.to_dict(orient="records")
            if ( (len(df_B)) != 0):
                plt_3 = go.bar(df_B_2, x="Sales Stage", y='TCV', color='Deal Size', hover_data=['Deal Size'], orientation='v',
                        template='plotly_dark', barmode="group",text='TCV')
                #plt.update_traces(texttemplate='%{text:.2s}')
                plt_3.update_traces(texttemplate='%{value:$,.1f}',textposition="outside")
                plt_3.update_layout(title="Summary of Opportunities in B and not in A")
                plt_3.update_layout(template='plotly_dark', autosize=False, width=1800,height=500)
                plot_div_3 = plot(plt_3,output_type='div', show_link=False)
                
                df_B_dum = df_B
                df_B_dum['Created Date'] =pd.to_datetime(df_B_dum['Created Date']).dt.strftime('%d-%m-%Y')
                df_B_dum['CloseDate'] =pd.to_datetime(df_B_dum['CloseDate']).dt.strftime('%d-%m-%Y')
                dealsData = df_B_dum.to_dict(orient="records")
                plt_4 = go.scatter(df_B, x='CloseDate', y='TCV', color='Sales Stage', size="TCV", hover_data=['Account Name','Opportunity Name','WinZone Opportunity ID'], template='plotly_dark')
                #plt_4.update_traces(texttemplate='%{value:$,.2s}', textposition='middle right')
                plt_4.update_layout(title="Deals in B and not in A",autosize=False, width=1800,height=500)
                plot_div_4 = plot(plt_4,output_type='div', show_link=False)

    try:
        cursor = connection.cursor()
        cursor.execute(sp_cl_stmt, ("Movement_Cre_Date",) + common_args1)
        # cursor.execute("select round(sum(isnull([Gross TCV $],0))/1000000,1) as TCV,[Created Date] from pipelinedata  where [Sales Stage] IN('1. Engagement','2. Shaping','3. Solutioning','4. End-Game','5. Negotiation') group by [Created Date]")
        col_names = [col_desc[0] for col_desc in cursor.description]
        df_cre = DataFrame.from_records(cursor.fetchall(), columns=col_names)
        if not df_cre.empty:
            df_cre.loc[:,"Created Date"] = pd.to_datetime(df_cre["Created Date"])
        
        max_cre_date = df_cre["Created Date"].max()
        new_cre_date = max_cre_date - pd.DateOffset(days=360)
        
        df_cre = df_cre[df_cre["Created Date"] >= new_cre_date]
        df_cre.sort_values(['Created Date'],axis=0,ascending=True,inplace = True)
        if not df_cre.empty:
            df_cre.loc[:,"Cumulative TCV"] = 0.0
        else:
            df_cre['Cumulative TCV'] = 0.0
        cumulative_total = 0.0
        for ele in range (len(df_cre)):
            cumulative_total = cumulative_total + df_cre["TCV"].values[ele]
            df_cre["Cumulative TCV"].values[ele] = cumulative_total
        cursor.execute(sp_cl_stmt, ("Movement_CloseWon_Date",) + common_args2)
        # cursor.execute("select round(sum(isnull([Gross TCV $],0))/1000000,1) as TCV,[CloseDate] from pipelinedata  where [Sales Stage] IN('Client Withdraw', 'Cognizant Withdraw', 'Won', 'Lost') group by [CloseDate]")
        col_names = [col_desc[0] for col_desc in cursor.description]
        df_close = DataFrame.from_records(cursor.fetchall(), columns=col_names)
        df_close.loc[:,"CloseDate"] = pd.to_datetime(df_close["CloseDate"])
        df_close = df_close[(df_close["CloseDate"] >= new_cre_date) & (df_close["CloseDate"] <= max_cre_date)]
        
        df_close.sort_values(['CloseDate'],axis=0,ascending=True,inplace = True)
        if not df_close.empty:
            df_close.loc[:,"Cumulative TCV"] = 0.0
        else:
            df_close["Cumulative TCV"] = 0.0
        cumulative_total = 0.0
        for ele in range (len(df_close)):
            cumulative_total = cumulative_total - df_close["TCV"].values[ele]
            df_close["Cumulative TCV"].values[ele] = cumulative_total
        cursor.execute(sp_cl_stmt, ("Movement_CloseWon_Date",) + common_args3)
        # cursor.execute("select round(sum(isnull([Gross TCV $],0))/1000000,1) as TCV,[Close Date] from pipelinedata  where [Sales Stage] IN('Won') group by [Close Date]")
        col_names = [col_desc[0] for col_desc in cursor.description]
        df_won = DataFrame.from_records(cursor.fetchall(), columns=col_names)

        df_won.loc[:,"CloseDate"] = pd.to_datetime(df_won["CloseDate"])
        df_won = df_won[(df_won["CloseDate"] >= new_cre_date) & (df_won["CloseDate"] <= max_cre_date)]
        
        df_won.sort_values(['CloseDate'],axis=0,ascending=True,inplace = True)
        if not df_won.empty:
            df_won.loc[:,"Cumulative TCV"] = 0.0
        else:
            df_won["Cumulative TCV"] = 0.0
        cumulative_total = 0.0
        for ele in range (len(df_won)):
            cumulative_total = cumulative_total - df_won["TCV"].values[ele]
            df_won["Cumulative TCV"].values[ele] = cumulative_total

        plt = go_2.Figure(data=go_2.Bar(x=df_close["CloseDate"], y=df_close.get("Cumulative TCV"), marker_color='red', name="All Closed $Cumulative TCV"))
        
        plt.add_trace(go_2.Scatter(x=df_won["CloseDate"], y=df_won.get("Cumulative TCV"), mode="markers+lines" , name="Won $TCV",
                    marker=dict(size=8, color="yellow", line=dict(width=2,color='DarkSlateGrey')), line=dict(dash='dot') ))
        plt.add_trace(go_2.Bar(x=df_cre["Created Date"], y=df_cre.get("Cumulative TCV"),  marker_color='salmon', name="All open opps $Cumulative TCV"))
        
        plt.update_layout(title="Pipeline Growth Profile - Ins (Stages 1 to 5) and Outs (Won/Lost/Withdrawn)")
        plt.update_layout(template='plotly_dark', autosize=False, width=1800,height=500)
        plt.update_yaxes(showgrid=False)
        plt.update_xaxes(showgrid=False)
        plot_div_6 = plot(plt,output_type='div', show_link=False)

        df_close['CloseDate'] =pd.to_datetime(df_close['CloseDate']).dt.strftime('%d-%m-%Y')
        # df_close['Cumulative TCV'] = df_close['Cumulative TCV'].round(1)
        closeData = df_close.to_dict(orient="records")
        df_cre['Created Date'] =pd.to_datetime(df_cre['Created Date']).dt.strftime('%d-%m-%Y')
        # closeData['Cumulative TCV'] = closeData['Cumulative TCV'].round(1)
        creationData = df_cre.to_dict(orient="records")
        df_won['CloseDate'] =pd.to_datetime(df_won['CloseDate']).dt.strftime('%d-%m-%Y')
        # df_won['Cumulative TCV'] = df_won['Cumulative TCV'].round(1)
        wonData = df_won.to_dict(orient="records")
    except:
        closeData=[]
        creationData=[]
        wonData=[]
    
    return render(request, 'visualize/show_movement.html', context={
        'plot_label':"Movement",
        'winzone_id':selected_values['selected_winzoneID'],
        # 'windata':json.dumps(windata),
        "showExtraTable":showExtraTable,
        'index_labels':index_labels,
        'index_labels_B':index_labels,
        'index_labels_A':index_labels,
        "idx_label_selected_A":idx_label_selected_A,
        "idx_label_selected_B":idx_label_selected_B,
        "plot_div_6":plot_div_6,
        "closeData":json.dumps(closeData),
        "creationData":json.dumps(creationData),
        "wonData":json.dumps(wonData),
        "dealsData":json.dumps(dealsData),
        "summaryData":json.dumps(summaryData),
        "movedData":json.dumps(movedData),
        "changeData":json.dumps(changeData),
        "totalData":json.dumps(totalData),
        "tableData":json.dumps(tableData),
        "tableColumns":json.dumps(tableColumns),
        "vertical_unique":values[0],
        "region_unique":values[1],
        "unique_mcu":values[2],
        "unique_sbu":values[3],
        "unique_account_name":values[4],
        "unique_horizontal":values[6],
        "sales_stage":values[8],
        "opp_type":values[7],
        "deal_size":["$0m - $2.5m", "$2.5m - $10m", "$10m - $25m", "$25m - $50m", ">= $50m"],
        "tier_data":values[5],
        "opt_opp_vertical_new":opt_opp_vertical_new,
        "opt_opp_region_new":opt_opp_region_new,
        "opt_sales_stage":opt_sales_stage,
        "opt_deal_size":opt_deal_size,
        "opt_mcu_checked":opt_mcu_checked,
        "opt_sbu_checked":opt_sbu_checked,
        "opt_account_name_checked":opt_account_name_checked,
        "opt_horizontal_checked":opt_horizontal_checked,
        "opt_opp_type":opt_opp_type,
        "opt_tier_data":opt_tier_data,
        "plot_div_change":plot_div_change,
        "plot_div":plot_div,
        "plot_div_pipe":plot_div_pipe,
        "plot_div_2":plot_div_2,
        "plot_div_3":plot_div_3,
        "plot_div_4":plot_div_4,
        "html_data_pivot":html_code,
        "resultDepData":json.dumps(values[9]),
        **selected_values,
        "opt_opp_region_new_filter":json.dumps(opt_opp_region_new)
    })
def show_pipeline_details_Acct_plot(index, label, request):
    # values = getDropDownValues()
    index_labels = getIndexLabels()
    idx_label_selected = request.GET.get("idx_label")
    if idx_label_selected == None:
        idx_label_selected = index_labels[0]
    values = getDropDownValues(idx_label_selected)
    selected_values = parse_selected_values(request,values[0],values[1],values[7],values[5],index_labels[0])
    # selected_values['idx_label_selected'] = datetime.strptime(selected_values['idx_label_selected'],'%b%d%Y').strftime('%m/%d/%Y')

    opt_opp_vertical_new = request.GET.getlist("vertical[]")
    if opt_opp_vertical_new == []:
        opt_opp_vertical_new = values[0]
    opt_opp_region_new = request.GET.getlist("unq_region[]")
    if opt_opp_region_new == []:
        opt_opp_region_new = values[1]

    stages = request.GET.getlist("sales_stage[]")
    cleaned = [s for s in stages if s != "Duplicate"]
    opt_sales_stage = cleaned
    if opt_sales_stage == []:
        opt_sales_stage = ["3. Solutioning", "4. End-Game", "5. Negotiation"]
    opt_deal_size = request.GET.getlist("deal_size[]")
    if opt_deal_size == []:
            opt_deal_size = ["$0m - $2.5m", "$2.5m - $10m", "$10m - $25m", "$25m - $50m", ">= $50m"]
    opt_tier_data = request.GET.getlist("tier_data[]")
    if opt_tier_data ==[]:
        opt_tier_data = values[5]
    opt_opp_type = request.GET.getlist("type[]")
    if opt_opp_type == []:
        opt_opp_type = values[7]
    opt_mcu_checked = request.GET.getlist("unique_mcu[]")
    if opt_mcu_checked == []:
        opt_mcu_checked = ["All"]
    opt_sbu_checked = request.GET.getlist("unique_sbu[]")
    if opt_sbu_checked == []:
        opt_sbu_checked = ["All"]
    opt_account_name_checked = request.GET.getlist("unique_account_name[]")
    if opt_account_name_checked == []:
        opt_account_name_checked = ["All"]
    opt_horizontal_checked = request.GET.getlist("unique_horizontal[]")
    if opt_horizontal_checked == []:
        opt_horizontal_checked = ["All"]

    
    all_deal_sizes = "$0m - $2.5m,$2.5m - $10m,$10m - $25m,$25m - $50m,>= $50m"
    
    sp_cl_stmt = "EXEC [dbo].[get_PipelineReport_Dev] @ptype= %s,@pvertical= %s,@pRegion= %s,@pBU= %s,@pSBU= %s,@pAccount= %s,@pHorizontal= %s,@pOpportunity= %s,@pSalesStage= %s,@pDealSize= %s,@pTier= %s,@ReportDt= %s,@WinzoneID=%s"
    common_args = (
        selected_values["selected_vertical"],
        selected_values["selected_region"],
        selected_values["selected_bu"],
        selected_values["selected_sbu"],
        selected_values["selected_account"],
        'All',
        selected_values["selected_oppType"],
        selected_values["selected_salesstage"],
        all_deal_sizes,
        selected_values["selected_tier"],
        selected_values['idx_label_selected'],
        selected_values['selected_winzoneID']
    )

    sp_cl_stmt_dev = "EXEC [dbo].[get_PipelineReport_Dev] @ptype=%s,@pvertical=%s,@pRegion=%s,@pBU=%s,@pSBU=%s,@pAccount=%s,@pHorizontal=%s,@pOpportunity=%s,@pSalesStage=%s,@pDealSize=%s,@pTier=%s,@ReportDt=%s,@WinzoneID=%s"
    common_args_dev = (
        selected_values["selected_vertical"],
        selected_values["selected_region"],
        selected_values["selected_bu"],
        selected_values["selected_sbu"],
        selected_values["selected_account"],
        'All',
        selected_values["selected_oppType"],
        selected_values["selected_salesstage"],
        selected_values["selected_dealsize"],
        selected_values["selected_tier"],
        selected_values['idx_label_selected'],
        ''
        # selected_values['selected_winzoneID']
    )
    # cursor = connection.cursor()
    # cursor.execute(sp_cl_stmt_dev, ("getUnique_Winzone",) + common_args_dev)
    # col_names = [col_desc[0] for col_desc in cursor.description]
    # windf = DataFrame.from_records(cursor.fetchall(), columns=col_names)
    # windata = windf['WinZone Opportunity ID'].values.tolist()

    try:
        cursor = connection.cursor()
        cursor.execute(sp_cl_stmt, ("getTotal_TCV_Count",) + common_args)
        # print(sp_cl_stmt, ("getTotal_TCV_Count",) + common_args)
        col_names = [col_desc[0] for col_desc in cursor.description]
        dfTotal = DataFrame.from_records(cursor.fetchall(), columns=col_names)
        dfTotalData = dfTotal.to_dict(orient="records")
        df_count = "{:,.0f}".format(int(dfTotalData[0]['WinZone Opportunity ID']))
        tcv_total = "{:,.1f}".format(dfTotalData[0]['TCV'])
    except:
        dfTotalData=[]
        df_count=0
        tcv_total=0

    try:
        cursor.execute(sp_cl_stmt, ("accounts",) + common_args)
        # cursor.execute("select [WinZone Opportunity ID],R_Vertical Vertical,[Account Name],[Opportunity Record Type Name],[Sales Stage],DealSize,round([Gross TCV $]/1000000,2) as TCV,CloseMonth [Close Date] from pipelinedata where [Sales Stage] IN('1. Engagement','2. Shaping','3. Solutioning','4. End-Game','5. Negotiation')") #sp_call_stmt2,('Base',)+common_args
        col_names = [col_desc[0] for col_desc in cursor.description]
        df = DataFrame.from_records(cursor.fetchall(), columns=col_names)
        tableData = df.to_dict(orient="records")
    except:
        tableData=[]
    return render(request, 'visualize/show_pipeline_details_account.html', context={
        'plot_label':request.GET.get("plt_label"),
        'winzone_id':selected_values['selected_winzoneID'],
        # 'windata':json.dumps(windata),
        'df_count':df_count,
        'tcv_total':tcv_total,
        'index_labels':index_labels,
        'idx_label_selected':idx_label_selected,
        'tableData':json.dumps(tableData),
        "vertical_unique":values[0],
        "region_unique":values[1],
        "unique_mcu":values[2],
        "unique_sbu":values[3],
        "unique_account_name":values[4],
        "unique_horizontal":values[6],
        "sales_stage":values[8],
        "opp_type":values[7],
        "deal_size":["$0m - $2.5m", "$2.5m - $10m", "$10m - $25m", "$25m - $50m", ">= $50m"],
        "tier_data":values[5],
        "opt_opp_vertical_new":opt_opp_vertical_new,
        "opt_opp_region_new":opt_opp_region_new,
        "opt_sales_stage":opt_sales_stage,
        "opt_deal_size":opt_deal_size,
        "opt_mcu_checked":opt_mcu_checked,
        "opt_sbu_checked":opt_sbu_checked,
        "opt_account_name_checked":opt_account_name_checked,
        "opt_horizontal_checked":opt_horizontal_checked,
        "opt_opp_type":opt_opp_type,
        "opt_tier_data":opt_tier_data,
        "resultDepData":json.dumps(values[9]),
        **selected_values,
        "opt_opp_region_new_filter":json.dumps(opt_opp_region_new)
    })
# @transaction.atomic
def show_won_lost_plot(label, request):
    # values = getDropDownValues()
    index_labels = getIndexLabels()
    idx_label_selected = request.GET.get("idx_label")
    if idx_label_selected == None:
        idx_label_selected = index_labels[0]
    values = getDropDownValues(idx_label_selected)
    selected_values = parse_selected_values(request,values[0],values[1],values[7],values[5],index_labels[0])
    # selected_values['idx_label_selected'] = datetime.strptime(selected_values['idx_label_selected'],'%b%d%Y').strftime('%m/%d/%Y')
    
    opt_opp_vertical_new = request.GET.getlist("vertical[]")
    if opt_opp_vertical_new == []:
        opt_opp_vertical_new = values[0]
    opt_opp_region_new = request.GET.getlist("region[]")
    if opt_opp_region_new == []:
        opt_opp_region_new = values[1]
    opt_sales_stage = ["Won","Lost"]
    if opt_sales_stage == []:
        opt_sales_stage = ["Won","Lost"]
    opt_deal_size = request.GET.getlist("deal_size[]")
    if opt_deal_size == []:
            opt_deal_size = ["$0m - $2.5m", "$2.5m - $10m", "$10m - $25m", "$25m - $50m", ">= $50m"]
    opt_tier_data = request.GET.getlist("tier_data[]")
    if opt_tier_data ==[]:
        opt_tier_data = values[5]
    opt_opp_type = request.GET.getlist("type[]")
    if opt_opp_type == []:
        opt_opp_type = values[7]
    opt_mcu_checked = request.GET.getlist("unique_mcu[]")
    if opt_mcu_checked == []:
        opt_mcu_checked = ["All"]
    opt_sbu_checked = request.GET.getlist("unique_sbu[]")
    if opt_sbu_checked == []:
        opt_sbu_checked = ["All"]
    opt_account_name_checked = request.GET.getlist("unique_account_name[]")
    if opt_account_name_checked == []:
        opt_account_name_checked = ["All"]
    opt_horizontal_checked = request.GET.getlist("unique_horizontal[]")
    if opt_horizontal_checked == []:
        opt_horizontal_checked = ["All"]
    
    sp_cl_stmt = "EXEC [dbo].[get_PipelineReport_Dev] @ptype= %s,@pvertical= %s,@pRegion= %s,@pBU= %s,@pSBU= %s,@pAccount= %s,@pHorizontal= %s,@pOpportunity= %s,@pSalesStage= %s,@pDealSize= %s,@pTier= %s,@ReportDt= %s,@WinzoneID=%s"
    common_args = (
        selected_values["selected_vertical"],
        selected_values["selected_region"],
        selected_values["selected_bu"],
        selected_values["selected_sbu"],
        selected_values["selected_account"],
        'All',
        selected_values["selected_oppType"],
        'Won,Lost',
        selected_values["selected_dealsize"],
        selected_values["selected_tier"],
        selected_values['idx_label_selected'],
        selected_values['selected_winzoneID']
    )
    sp_cl_stmt_dev = "EXEC [dbo].[get_PipelineReport_Dev] @ptype=%s,@pvertical=%s,@pRegion=%s,@pBU=%s,@pSBU=%s,@pAccount=%s,@pHorizontal=%s,@pOpportunity=%s,@pSalesStage=%s,@pDealSize=%s,@pTier=%s,@ReportDt=%s,@WinzoneID=%s"
    common_args_dev = (
        selected_values["selected_vertical"],
        selected_values["selected_region"],
        selected_values["selected_bu"],
        selected_values["selected_sbu"],
        selected_values["selected_account"],
        'All',
        selected_values["selected_oppType"],
        selected_values["selected_salesstage"],
        selected_values["selected_dealsize"],
        selected_values["selected_tier"],
        selected_values['idx_label_selected'],
        ''
        # selected_values['selected_winzoneID']
    )
    # cursor = connection.cursor()
    # cursor.execute(sp_cl_stmt_dev, ("getUnique_Winzone",) + common_args_dev)
    # col_names = [col_desc[0] for col_desc in cursor.description]
    # windf = DataFrame.from_records(cursor.fetchall(), columns=col_names)
    # windata = windf['WinZone Opportunity ID'].values.tolist()

    try:
        cursor = connection.cursor()
        cursor.execute(sp_cl_stmt, ("getTotal_TCV_Count",) + common_args)
        col_names = [col_desc[0] for col_desc in cursor.description]
        dfTotal = DataFrame.from_records(cursor.fetchall(), columns=col_names)
        dfTotalData = dfTotal.to_dict(orient="records")
        df_count = "{:,.0f}".format(int(dfTotalData[0]['WinZone Opportunity ID']))
        tcv_total = "{:,.1f}".format(dfTotalData[0]['TCV'])
    except:
        dfTotalData=[]
        df_count=0
        tcv_total=0
    
    # try:
    #     # print('start',datetime.now())
    #     cursor.execute(sp_cl_stmt, ("WonLost",) + common_args)
    #     # print('start===1826',datetime.now())
    #     # cursor.execute("select top 1000 [WinZone Opportunity ID],R_Vertical Vertical,[Account Name],[Opportunity Record Type Name],[Sales Stage],DealSize,round([Gross TCV $]/1000000,2) as TCV,CloseMonth [Close Date] from pipelinedata where [Sales Stage] IN('Won','Lost')") #sp_call_stmt2,('Base',)+common_args
    #     col_names = [col_desc[0] for col_desc in cursor.description]
    #     # print('start===1829',datetime.now())
    #     df = DataFrame.from_records(cursor.fetchall(), columns=col_names)
    #     # print('start===1831',datetime.now())
    #     tableData = df.to_dict(orient="records")
    #     # print('end',datetime.now())
    # except:
    tableData=[]
    return render(request, 'visualize/show_pipeline_details_wonLost_new.html', context={'plot_label':request.GET.get("plt_label"),
        'index_labels':index_labels,
        'winzone_id':selected_values['selected_winzoneID'],
        # 'windata':json.dumps(windata),
        'df_count':df_count,
        'tcv_total':tcv_total,
        'idx_label_selected':idx_label_selected,
        'tableData':json.dumps(tableData),
        "vertical_unique":values[0],
        "region_unique":values[1],
        "unique_mcu":values[2],
        "unique_sbu":values[3],
        "unique_account_name":values[4],
        "unique_horizontal":values[6],
        "sales_stage":values[8],
        "opp_type":values[7],
        "deal_size":["$0m - $2.5m", "$2.5m - $10m", "$10m - $25m", "$25m - $50m", ">= $50m"],
        "tier_data":values[5],
        "opt_opp_vertical_new":opt_opp_vertical_new,
        "opt_opp_region_new":opt_opp_region_new,
        "opt_sales_stage":opt_sales_stage,
        "opt_deal_size":opt_deal_size,
        "opt_mcu_checked":opt_mcu_checked,
        "opt_sbu_checked":opt_sbu_checked,
        "opt_account_name_checked":opt_account_name_checked,
        "opt_horizontal_checked":opt_horizontal_checked,
        "opt_opp_type":opt_opp_type,
        "opt_tier_data":opt_tier_data,
        "resultDepData":json.dumps(values[9]),
        **selected_values,
        "opt_opp_region_new_filter":json.dumps(opt_opp_region_new)
        })

def show_key_metrices_plot(label,index,request):
    # values = getDropDownValues()
    index_labels = getIndexLabels()
    idx_label_selected = request.GET.get("idx_label")
    if idx_label_selected == None:
        idx_label_selected = index_labels[0]
    values = getDropDownValues(idx_label_selected)
    selected_values = parse_selected_values(request,values[0],values[1],values[7],values[5],index_labels[0])
    # selected_values['idx_label_selected'] = datetime.strptime(selected_values['idx_label_selected'],'%b%d%Y').strftime('%m/%d/%Y')
    
    opt_opp_vertical_new = request.GET.getlist("vertical[]")
    if opt_opp_vertical_new == []:
        opt_opp_vertical_new = values[0]
    opt_opp_region_new = request.GET.getlist("unq_region[]")
    if opt_opp_region_new == []:
        opt_opp_region_new = values[1]
    
    stages = request.GET.getlist("sales_stage[]")
    cleaned = [s for s in stages if s != "Duplicate"]
    opt_sales_stage = cleaned
    if opt_sales_stage == []:
        opt_sales_stage = ["3. Solutioning", "4. End-Game", "5. Negotiation"]
    opt_deal_size = request.GET.getlist("deal_size[]")
    if opt_deal_size == []:
            opt_deal_size = ["$0m - $2.5m", "$2.5m - $10m", "$10m - $25m", "$25m - $50m", ">= $50m"]
    opt_tier_data = request.GET.getlist("tier_data[]")
    if opt_tier_data ==[]:
        opt_tier_data = values[5]
    opt_opp_type = request.GET.getlist("type[]")
    if opt_opp_type == []:
        opt_opp_type = values[7]
    opt_mcu_checked = request.GET.getlist("unique_mcu[]")
    if opt_mcu_checked == []:
        opt_mcu_checked = ["All"]
    opt_sbu_checked = request.GET.getlist("unique_sbu[]")
    if opt_sbu_checked == []:
        opt_sbu_checked = ["All"]
    opt_account_name_checked = request.GET.getlist("unique_account_name[]")
    if opt_account_name_checked == []:
        opt_account_name_checked = ["All"]
    opt_horizontal_checked = request.GET.getlist("unique_horizontal[]")
    if opt_horizontal_checked == []:
        opt_horizontal_checked = ["All"]
    
    sp_cl_stmt = "EXEC [dbo].[get_PipelineReport] @ptype= %s,@pvertical= %s,@pRegion= %s,@pBU= %s,@pSBU= %s,@pAccount= %s,@pHorizontal= %s,@pOpportunity= %s,@pSalesStage= %s,@pDealSize= %s,@pTier= %s,@ReportDt= %s"
    common_args = (
        selected_values["selected_vertical"],
        selected_values["selected_region"],
        selected_values["selected_bu"],
        selected_values["selected_sbu"],
        selected_values["selected_account"],
        'All',
        selected_values["selected_oppType"],
        selected_values["selected_salesstage"],
        selected_values["selected_dealsize"],
        selected_values["selected_tier"],
        selected_values['idx_label_selected']
    )
    cursor = connection.cursor()
    cursor.execute(sp_cl_stmt, ("Metrics_ByDealSize",) + common_args)
    # cursor.execute("select [WinZone Opportunity ID],R_Vertical Vertical,[R_Market Unit] Region,'' WinZoneRegion,SBU1,'' MCU,[Account Name],[Opportunity Record Type Name],Practice Horizontal,[Sales Stage],[R_Deal Type] Type,round([Gross TCV $]/1000000,2) TCV,[Deal Duration (Months)], [Win Probability (%)],[Pursuit Lead],DealSize,[Competitors],[Created Date],CloseDate,CloseQtr,year(CloseDate) [Close Year], datediff(dd,[Created Date],CloseDate) [Cycle time] from pipelinedata where [Sales Stage] IN('1. Engagement','2. Shaping','3. Solutioning','4. End-Game','5. Negotiation')")
    # cursor.execute("get_Pipeline @ptype = 'Metrics_ByDealSize'")
    col_names = [col_desc[0] for col_desc in cursor.description]
    df_service_line = DataFrame.from_records(cursor.fetchall(), columns=col_names)
    # df_service_line = df_service_line.fillna('')
    new_list = []
    for y in range(len(df_service_line)):
        value = df_service_line["Practice"].values[y]
        if(value is not None):
            pa_list = value.split(';')
        else:
            pa_list =[]
        #print(pa_list)
        if(len(pa_list) > 0):
            
            for i in range(len(pa_list)):
                new_label = pa_list[i]
                row_list = []
                if ((new_label.lower() != "null") & (new_label != '')):
                    row_list.append(df_service_line["Vertical"].values[y])
                    row_list.append(df_service_line["Region"].values[y])
                    row_list.append(df_service_line["TCV"].values[y])
                    row_list.append(df_service_line["DealSize"].values[y])
                    row_list.append(df_service_line["WinZone Opportunity ID"].values[y])
                    row_list.append(new_label)
                    #print(row_list)
                    new_list.append(row_list)
    # print(y)
    new_df_service_line = pd.DataFrame(new_list,columns=["Vertical","Region","TCV","DealSize","WinZone Opportunity ID","Practice"])

    # new_df_service_line = new_df_service_line.rename(columns={"Region": "WinZone Region"})
    # df_service_line = new_df_service_line.groupby(["Region","Vertical","DealSize","Horizontal"], as_index=False).agg(
    #        { "TCV": sum,
    #           "WinZone Opportunity ID": "count",
    #         }
    #     ) 

    plt = go.bar(new_df_service_line, x='Practice', y='TCV', color='DealSize', hover_data=["WinZone Opportunity ID","Vertical"], template='plotly_dark', barmode="stack",text='TCV')
    plt.update_traces(texttemplate='%{text:.2s}', textposition='outside')
    # plt.update_layout(title= title + " " +"Pipeline By Deal Size", autosize=False, width=1800,height=500)
    plt.update_layout(title= "Pipeline By Deal Size", autosize=False, width=1800,height=500)
    plot_div_1 = plot(plt,output_type='div', show_link=False)

    cursor.execute(sp_cl_stmt, ("Metrics_ByCompetition",) + common_args)
    # cursor.execute("get_Pipeline @ptype = 'Metrics_ByCompetition'")
    col_names = [col_desc[0] for col_desc in cursor.description]
    df_competition = DataFrame.from_records(cursor.fetchall(), columns=col_names)
    new_list_competition = []
    for j in range(len(df_competition)):
        value = df_competition["Competitors"].values[j]
        if(value is not None):
            pa_list_competition = value.split(';')
        else:
            pa_list_competition =[]
        # .split(';')
        if(len(pa_list_competition) > 0):
            
            for k in range(len(pa_list_competition)):
                new_label_competition = pa_list_competition[k]
                row_list_competition = []
                if ((new_label_competition.lower() != "null") & (new_label_competition != '')):
                    row_list_competition.append(df_competition["Vertical"].values[j])
                    row_list_competition.append(df_competition["Region"].values[j])
                    row_list_competition.append(df_competition["DealSize"].values[j])
                    row_list_competition.append(df_competition["WinZone Opportunity ID"].values[j])
                    row_list_competition.append(df_competition["TCV"].values[j])
                    row_list_competition.append(new_label_competition)
                    new_list_competition.append(row_list_competition)
    new_df_competition = pd.DataFrame(new_list_competition,columns=["Vertical","Region","DealSize","WinZone Opportunity ID","TCV","Competitors"])

    plt = go.bar(new_df_competition, x='Competitors', y='TCV', color='DealSize', hover_data=["WinZone Opportunity ID","Vertical"], template='plotly_dark', barmode="stack",text='TCV')
    plt.update_traces(texttemplate='%{text:.2s}', textposition='outside')
    # plt.update_layout(title= title + " " + "Pipeline By Deal Size", autosize=False, width=1800,height=500)
    plt.update_layout(title= "Pipeline By Deal Size", autosize=False, width=1800,height=500)
    plot_div_2 = plot(plt,output_type='div', show_link=False)
    # df_pivot_competition = pd.pivot_table(df_competition,values=["TCV"],index=["Vertical","Region","Deal Size", "WinZone Region"],columns=["Competitor"], aggfunc={"TCV":numpy.sum}, fill_value=0, margins=True,margins_name="Totals")
    
    # if len(new_df_competition.index) > 0:
    #     df_competition = new_df_competition.groupby(["Region","Vertical","Deal Size","Competitor", "Region1"], as_index=False).agg(
    #         { "TCV": sum,
    #            "WZ ID": "count",
    #         }
    #     )



    # cursor.execute("get_Pipeline @ptype = 'Metrics_ByAVGDeal'"
    cursor.execute(sp_cl_stmt, ("Metrics_ByAVGDeal",) + common_args)
    col_names = [col_desc[0] for col_desc in cursor.description]
    df_avg_deal_size = DataFrame.from_records(cursor.fetchall(), columns=col_names)
    plt = go.bar(df_avg_deal_size, x='CloseQtr', y='TCV', color='DealSize', hover_data=['WinZone Opportunity ID','Vertical'], template='plotly_dark', barmode="group",text='TCV')
    plt.update_traces(texttemplate='%{text:.2s}', textposition='outside')
    # plt.update_layout(title= title + " Pipeline By Deal Size", autosize=False, width=1800,height=500)
    plt.update_layout(title=" Pipeline By Deal Size", autosize=False, width=1800,height=500)
    plot_div_3 = plot(plt,output_type='div', show_link=False)

    # cursor.execute("get_Pipeline @ptype = 'CycleTime'")
    cursor.execute(sp_cl_stmt, ("CycleTime",) + common_args)
    col_names = [col_desc[0] for col_desc in cursor.description]
    df_avg_cycle = DataFrame.from_records(cursor.fetchall(), columns=col_names)
    df_avg_cycle = df_avg_cycle.rename(columns={"Mean":"Cycle Time"})

    max_cycle_time = df_avg_cycle["Cycle Time"].max()
    plt = go.bar(df_avg_cycle, x='DealSize', y='Cycle Time', color='Sales Stage', hover_data=['Sales Stage','Vertical'],
    template='plotly_dark', facet_row="Vertical", animation_frame="Region", facet_row_spacing=0.01, range_y=[0,max_cycle_time],barmode="group",text='Cycle Time')
    plt.update_traces(texttemplate='%{text:.2s}', textposition='outside')
    plt.update_layout(title="Cycle Time By Deal Size",barmode='group', width=1800,height=2400)
    plot_div_4 = plot(plt,output_type='div',image_height=600, image_width=1500, show_link=False)
    return render(request, 'visualize/show_key_metrices.html', context={
        'plot_label':request.GET.get("plt_label"),
        'index_labels':index_labels,
        'idx_label_selected':idx_label_selected,
        'plot_div_1':plot_div_1,
        'plot_div_2':plot_div_2,
        'plot_div_3':plot_div_3,
        'plot_div_4':plot_div_4,
        "vertical_unique":values[0],
        "region_unique":values[1],
        "unique_mcu":values[2],
        "unique_sbu":values[3],
        "unique_account_name":values[4],
        "unique_horizontal":values[6],
        "sales_stage":values[8],
        "opp_type":values[7],
        "deal_size":["$0m - $2.5m", "$2.5m - $10m", "$10m - $25m", "$25m - $50m", ">= $50m"],
        "tier_data":values[5],
        "opt_opp_vertical_new":opt_opp_vertical_new,
        "opt_opp_region_new":opt_opp_region_new,
        "opt_sales_stage":opt_sales_stage,
        "opt_deal_size":opt_deal_size,
        "opt_mcu_checked":opt_mcu_checked,
        "opt_sbu_checked":opt_sbu_checked,
        "opt_account_name_checked":opt_account_name_checked,
        "opt_horizontal_checked":opt_horizontal_checked,
        "opt_opp_type":opt_opp_type,
        "opt_tier_data":opt_tier_data
    })

def AjaxCallForDashBoard(request):
    idx_label = request.GET.get("idx_label")
    unique_mcu = ','.join(request.GET.getlist("unique_mcu[]"))
    unique_sbu = ','.join(request.GET.getlist("unique_sbu[]"))
    unique_account_name = ','.join(request.GET.getlist("unique_account_name[]"))
    vertical = request.GET.get("vertical")
    salesstage = request.GET.get("salesstage")
    market = request.GET.get("market")
    winzone_id = request.GET.get("winzone_id")
    dealsize = request.GET.get("dealsize")
    opptype = request.GET.get("opptype")
    tierdata = request.GET.get("tierdata")
    # print(vertical,'\n',idx_label,'\n',unique_mcu,'\n',unique_sbu,'\n',unique_account_name,'\n',salesstage,'\n',market,'\n',winzone_id
    # ,'\n',dealsize,'\n',opptype,'\n',tierdata)
    sp_cl_stmt = "EXEC [dbo].[get_PipelineReport_Dev] @ptype= %s,@pvertical= %s,@pRegion= %s,@pBU= %s,@pSBU= %s,@pAccount= %s,@pHorizontal= %s,@pOpportunity= %s,@pSalesStage= %s,@pDealSize= %s,@pTier= %s,@ReportDt= %s,@WinzoneID=%s"
    common_args3 = (
        vertical,
        market,
        unique_mcu,
        unique_sbu,
        unique_account_name,
        'All',
        opptype,
        "All",
        dealsize,
        tierdata,
        idx_label,
        winzone_id
    )
    common_args2 = (
        vertical,
        market,
        unique_mcu,
        unique_sbu,
        unique_account_name,
        'All',
        opptype,
        'Won,Lost',
        dealsize,
        tierdata,
        idx_label,
        winzone_id
    )
    if salesstage == 'Lost,Won':
            cursor = connection.cursor()
            cursor.execute(sp_cl_stmt, ("WonLostDetails",) + common_args2)
            col_names = [col_desc[0] for col_desc in cursor.description]
            df4 = DataFrame.from_records(cursor.fetchall(), columns=col_names)
            df4['TCV'] = df4['TCV'].round(1)
            wonlostdf = df4
            # wonlostData = [] 
            wonlostdf.to_dict(orient="records")
            dealInPipeline = []
            accountData = []
    else:
        with connection.cursor() as cursor:
        # cursor = connection.cursor()
            try:
                cursor.execute(sp_cl_stmt, ("WonLostDetails",) + common_args3)
                col_names = [col_desc[0] for col_desc in cursor.description]
                df4 = DataFrame.from_records(cursor.fetchall(), columns=col_names)
                df4['TCV'] = df4['TCV'].round(1)
                wonlostdf = df4
                wonlostdf = wonlostdf[(wonlostdf["Sales Stage"].isin(['Lost','Won']))]
                wonlostData = wonlostdf.to_dict(orient="records")

                selectSalesValues = request.GET.getlist("sales_stage[]")or ["3. Solutioning", "4. End-Game", "5. Negotiation"]
                dealinpipeDf = df4
                dealinpipeDf = dealinpipeDf[(dealinpipeDf["Sales Stage"].isin(selectSalesValues))]
                dealInPipeline = dealinpipeDf.to_dict(orient="records")
                
                accountData = dealInPipeline
            except:
                wonlostData = [] 
                dealInPipeline = []
                accountData = []
    # cursor = connection.cursor()
    # cursor.execute(sp_cl_stmt, ("WonLostDetails",) + common_args3)
    # col_names = [col_desc[0] for col_desc in cursor.description]
    # df4 = DataFrame.from_records(cursor.fetchall(), columns=col_names)
    # df4['TCV'] = df4['TCV'].round(1)
    # wonlostdf = df4
    # wonlostdf = wonlostdf[(wonlostdf["Sales Stage"].isin(['Lost','Won']))]
    # wonlostData = wonlostdf.to_dict(orient="records")

    # selectSalesValues = request.GET.getlist("sales_stage[]")or ["3. Solutioning", "4. End-Game", "5. Negotiation"]
    # dealinpipeDf = df4
    # dealinpipeDf = dealinpipeDf[(dealinpipeDf["Sales Stage"].isin(selectSalesValues))]
    # dealInPipeline = dealinpipeDf.to_dict(orient="records")
    
    # accountData = dealInPipeline
    return HttpResponse(json.dumps({"accountData": accountData,
    'dealInPipeline':dealInPipeline,
    'wonlostData':wonlostData,
    }))

# def AjaxCallForIndexLabel(request):
#     idx_label = request.GET.get("idx_label")
#     print('idx_label======',idx_label)
#     values = getDropDownValues(idx_label)
#     selected_values = parse_selected_values(request,values[0],values[1],values[7],values[5],idx_label)
#     print(selected_values)
#     opt_opp_region_new = request.GET.getlist("unq_region[]")
#     # print('unq_region',opt_opp_region_new)
#     if opt_opp_region_new == []:
#         opt_opp_region_new = values[1]
#     return HttpResponse(json.dumps({
#         **selected_values,
#         "resultDepData":json.dumps(values[9]),
#         "opt_opp_region_new_filter":json.dumps(opt_opp_region_new)
#     }))

def AjaxCallForWonLost(request):
    idx_label = request.GET.get("idx_label")
    unique_mcu = ','.join(request.GET.getlist("unique_mcu[]"))
    unique_sbu = ','.join(request.GET.getlist("unique_sbu[]"))
    unique_account_name = ','.join(request.GET.getlist("unique_account_name[]"))
    vertical = request.GET.get("vertical")
    salesstage = request.GET.get("salesstage")
    market = request.GET.get("market")
    winzone_id = request.GET.get("winzone_id")
    dealsize = request.GET.get("dealsize")
    opptype = request.GET.get("opptype")
    tierdata = request.GET.get("tierdata")
    
    sp_cl_stmt = "EXEC [dbo].[get_PipelineReport_Dev] @ptype= %s,@pvertical= %s,@pRegion= %s,@pBU= %s,@pSBU= %s,@pAccount= %s,@pHorizontal= %s,@pOpportunity= %s,@pSalesStage= %s,@pDealSize= %s,@pTier= %s,@ReportDt= %s,@WinzoneID=%s"
    common_args = (
        vertical,
        market,
        unique_mcu,
        unique_sbu,
        unique_account_name,
        'All',
        opptype,
        'Won,Lost',
        dealsize,
        tierdata,
        idx_label,
        winzone_id
    )
    try:
        cursor=connection.cursor()
        cursor.execute(sp_cl_stmt, ("WonLost",) + common_args)
        col_names = [col_desc[0] for col_desc in cursor.description]
        df = DataFrame.from_records(cursor.fetchall(), columns=col_names)
        tableData = df.to_dict(orient="records")
    except:
        tableData=[]
    
    return HttpResponse(json.dumps({
        "tableData":tableData
    }))

def EmeaReport(request):
    ##################
    # values = getDropDownValues()
    index_labels = getIndexLabels()
    idx_label_selected = request.GET.get("idx_label")
    if idx_label_selected == None:
        idx_label_selected = index_labels[0]
    values = getDropDownValues(idx_label_selected)
    selected_values = parse_selected_valueswinreport(request,values[0],values[1],values[7],values[5],index_labels[0])
    # print(selected_values)
    # selected_values['idx_label_selected'] = datetime.strptime(selected_values['idx_label_selected'],'%b%d%Y').strftime('%m/%d/%Y')
    
    opt_opp_vertical_new = request.GET.getlist("vertical[]")
    if opt_opp_vertical_new == []:
        opt_opp_vertical_new = values[0]
    opt_opp_region_new = request.GET.getlist("unq_region[]")
    # print(opt_opp_region_new,"2350")
    if opt_opp_region_new == []:
        opt_opp_region_new = values[1]
    
    stages = request.GET.getlist("sales_stage[]")
    cleaned = [s for s in stages if s != "Duplicate"]
    opt_sales_stage = cleaned
    # print(opt_sales_stage,'2354')
    if opt_sales_stage == []:
        opt_sales_stage = ['3. Solutioning','4. End-Game','5. Negotiation']
    
    stages = request.GET.getlist("sales_stageNew[]")
    cleaned = [s for s in stages if s != "Duplicate"]
    opt_sales_stageNew = cleaned
    # print('opt_sales_stageNew',opt_sales_stageNew)
    if opt_sales_stageNew == []:
        opt_sales_stageNew = ['1. Engagement','2. Shaping','Client Withdraw','Cognizant Withdraw','3. Solutioning','4. End-Game','5. Negotiation',"Won","Lost"]
    opt_deal_size = request.GET.getlist("deal_size[]")
    if opt_deal_size == []:
            opt_deal_size = ["$0m - $2.5m", "$2.5m - $10m", "$10m - $25m", "$25m - $50m", ">= $50m"]
    opt_tier_data = request.GET.getlist("tier_data[]")
    if opt_tier_data ==[]:
        opt_tier_data = values[5]
    opt_opp_type = request.GET.getlist("type[]")
    if opt_opp_type == []:
        opt_opp_type = values[7]
    opt_mcu_checked = request.GET.getlist("unique_mcu[]")
    if opt_mcu_checked == []:
        opt_mcu_checked = ["All"]
    opt_sbu_checked = request.GET.getlist("unique_sbu[]")
    if opt_sbu_checked == []:
        opt_sbu_checked = ["All"]
    opt_account_name_checked = request.GET.getlist("unique_account_name[]")
    if opt_account_name_checked == []:
        opt_account_name_checked = ["All"]
    opt_horizontal_checked = request.GET.getlist("unique_horizontal[]")
    if opt_horizontal_checked == []:
        opt_horizontal_checked = ["All"]
    ##################
    # sp_cl_stmt = "EXEC [dbo].[sp_EMEADeliveryConnect_Dev] @ptype= %s,@pvertical= %s,@pRegion= %s,@pBU= %s,@pSBU= %s,@pAccount= %s,@pHorizontal= %s,@pOpportunity= %s,@pSalesStage= %s,@pDealSize= %s,@pTier= %s,@ReportDt= %s,@pOppSource =%s ,@pAccType=%s"
    sp_cl_stmt = "Exec [dbo].[sp_EMEADeliveryConnect_Dev] @ptype =%s,@pvertical =%s,@pRegion =%s,@pBU =%s,@pSBU =%s,@pAccount =%s,@pHorizontal =%s,@pOpportunity =%s,@pSalesStage =%s,@pDealSize =%s,@pTier =%s,@ReportDt =%s,@WinzoneID =%s,@pOppSource =%s ,@pAccType=%s"
    common_args = (
        selected_values["selected_vertical"],
        selected_values["selected_region"],
        selected_values["selected_bu"],
        selected_values["selected_sbu"],
        selected_values["selected_account"],
        'All',
        selected_values["selected_oppType"],
        selected_values["selected_salesstage"],
        selected_values["selected_dealsize"],
        selected_values["selected_tier"],
        selected_values['idx_label_selected'],
        selected_values['selected_winzoneID'],
        selected_values['selected_opp_source'],
        selected_values['selected_acc_type']
    )
    # print(common_args)
    print(sp_cl_stmt, ("Report",) + common_args)
    cursor = connection.cursor()
    cursor.execute(sp_cl_stmt, ("Report",) + common_args)
    col_names = [col_desc[0] for col_desc in cursor.description]
    # df_service_line = DataFrame.from_records(cursor.fetchall(), columns=col_names)
    # cursor=connection.cursor()
    # sp_cl_stmt = "sp_EMEADeliveryConnect 'Report'"
    # cursor.execute(sp_cl_stmt)
    # col_names = [col_desc[0] for col_desc in cursor.description]
    df = DataFrame.from_records(cursor.fetchall(), columns=col_names)
    tableData = df.to_dict(orient="records")
    # print(tableData)
    # sp_cl_stmt = "sp_EMEADeliveryConnect 'WonTable'"
    # cursor.execute(sp_cl_stmt, ("WonTable",) + common_args)
    # col_names = [col_desc[0] for col_desc in cursor.description]
    # cursor.execute(sp_cl_stmt)
    # col_names = [col_desc[0] for col_desc in cursor.description]
    # df = DataFrame.from_records(cursor.fetchall(), columns=col_names)
    # WinstableData = df.to_dict(orient="records")
    ##
    # sp_cl_stmt = "sp_EMEADeliveryConnect 'AVGTCV'"
    # cursor.execute(sp_cl_stmt)
    # col_names = [col_desc[0] for col_desc in cursor.description]
    # cursor.execute(sp_cl_stmt, ("AVGTCV",) + common_args)
    # col_names = [col_desc[0] for col_desc in cursor.description]
    # df = DataFrame.from_records(cursor.fetchall(), columns=col_names)
    # AvgtableData = df.to_dict(orient="records")
    ###
    # sp_cl_stmt = "sp_EMEADeliveryConnect 'PipelineTbl'"
    # cursor.execute(sp_cl_stmt)
    # cursor.execute(sp_cl_stmt, ("PipelineTbl",) + common_args)
    # col_names = [col_desc[0] for col_desc in cursor.description]
    # df = DataFrame.from_records(cursor.fetchall(), columns=col_names)
    # PipelinetableData = df.to_dict(orient="records")
    ###
    # sp_cl_stmt = "sp_EMEADeliveryConnect 'QualPipelineTbl'"
    # cursor.execute(sp_cl_stmt)
    # cursor.execute(sp_cl_stmt, ("QualPipeTable",) + common_args)
    # col_names = [col_desc[0] for col_desc in cursor.description]
    # combinedf = DataFrame.from_records(cursor.fetchall(), columns=col_names)
    # CombineQualPipetableData = combinedf.to_dict(orient="records")
    #############################
    # cursor.execute(sp_cl_stmt, ("QualPipelineTbl",) + common_args)
    # col_names = [col_desc[0] for col_desc in cursor.description]
    # df = DataFrame.from_records(cursor.fetchall(), columns=col_names)
    # QualPipetableData = df.to_dict(orient="records")
    return render(request, 'visualize/EmeaReports.html',{'tableData':json.dumps(tableData),'plot_label':'Win Report',
    # 'WinstableData':json.dumps(WinstableData),
    # 'QualPipetableData':json.dumps(QualPipetableData),
    # 'PipelinetableData':json.dumps(PipelinetableData),
    # 'AvgtableData':json.dumps(AvgtableData),
    # 'CombineQualPipetableData':json.dumps(CombineQualPipetableData),
    'plot_label':request.GET.get("plt_label") or 'Win Report',
        'index_labels':index_labels,
        'winzone_id':selected_values['selected_winzoneID'],
        # 'windata':json.dumps(windata),
        # 'df_count':df_count,
        # 'tcv_total':tcv_total,
        'idx_label_selected':idx_label_selected,
        # 'tableData':json.dumps(tableData),
        "vertical_unique":values[0],
        "region_unique":values[1],
        "unique_mcu":values[2],
        "unique_sbu":values[3],
        "unique_account_name":values[4],
        "unique_horizontal":values[6],
        "sales_stage":values[8],
        "sales_stageNew":values[8],
        "opp_type":values[7],
        "deal_size":["$0m - $2.5m", "$2.5m - $10m", "$10m - $25m", "$25m - $50m", ">= $50m"],
        "tier_data":values[5],
        "opt_opp_vertical_new":opt_opp_vertical_new,
        "opt_opp_region_new":opt_opp_region_new,
        "opt_sales_stage":opt_sales_stage,
        "opt_sales_stageNew":opt_sales_stageNew,
        "opt_deal_size":opt_deal_size,
        "opt_mcu_checked":opt_mcu_checked,
        "opt_sbu_checked":opt_sbu_checked,
        "opt_account_name_checked":opt_account_name_checked,
        "opt_horizontal_checked":opt_horizontal_checked,
        "opt_opp_type":opt_opp_type,
        "opt_tier_data":opt_tier_data,
        "resultDepData":json.dumps(values[9]),
        **selected_values,
        "opt_opp_region_new_filter":json.dumps(opt_opp_region_new)})

def AjaxCallForWinstableData(request):
        # print("inwinstable")
        idx_label = request.GET.get("idx_label")
        unique_mcu = ','.join(request.GET.getlist("unique_mcu[]"))
        unique_sbu = ','.join(request.GET.getlist("unique_sbu[]"))
        opp_Source = ','.join(request.GET.getlist("opp_Source[]"))
        Acc_Type = ','.join(request.GET.getlist("Acc_Type[]"))
        unique_account_name = ','.join(request.GET.getlist("unique_account_name[]"))
        vertical = request.GET.get("vertical")
        salesstage = request.GET.get("salesstage")
        market = request.GET.get("market")
        winzone_id = request.GET.get("winzone_id")
        dealsize = request.GET.get("dealsize")
        opptype = request.GET.get("opptype")
        tierdata = request.GET.get("tierdata")
        # print(salesstage)
        sp_cl_stmt = "Exec [dbo].[sp_EMEADeliveryConnect_Dev] @ptype =%s,@pvertical =%s,@pRegion =%s,@pBU =%s,@pSBU =%s,@pAccount =%s,@pHorizontal =%s,@pOpportunity =%s,@pSalesStage =%s,@pDealSize =%s,@pTier =%s,@ReportDt =%s,@WinzoneID =%s,@pOppSource =%s ,@pAccType=%s"

        common_args = (
            vertical,
            market,
            unique_mcu,
            unique_sbu,
            unique_account_name,
            'All',
            opptype,
            salesstage,
            dealsize,
            tierdata,
            idx_label,
            winzone_id,
            opp_Source,
            Acc_Type
        )
        # print(common_args,"2536")
        try:
            cursor=connection.cursor()
            cursor.execute(sp_cl_stmt, ("WonTable",) + common_args)
            col_names = [col_desc[0] for col_desc in cursor.description]
            df = DataFrame.from_records(cursor.fetchall(), columns=col_names)
            tableData = df.to_dict(orient="records")
        except:
            tableData=[]
        return HttpResponse(json.dumps({
            "tableData":tableData
        }))
def AjaxCallForAvgWinstableData(request):
        idx_label = request.GET.get("idx_label")
        unique_mcu = ','.join(request.GET.getlist("unique_mcu[]"))
        unique_sbu = ','.join(request.GET.getlist("unique_sbu[]"))
        opp_Source = ','.join(request.GET.getlist("opp_Source[]"))
        Acc_Type = ','.join(request.GET.getlist("Acc_Type[]"))
        unique_account_name = ','.join(request.GET.getlist("unique_account_name[]"))
        vertical = request.GET.get("vertical")
        salesstage = request.GET.get("salesstage")
        market = request.GET.get("market")
        winzone_id = request.GET.get("winzone_id")
        dealsize = request.GET.get("dealsize")
        opptype = request.GET.get("opptype")
        tierdata = request.GET.get("tierdata")
        # print(salesstage)
        sp_cl_stmt = "Exec [dbo].[sp_EMEADeliveryConnect_Dev] @ptype =%s,@pvertical =%s,@pRegion =%s,@pBU =%s,@pSBU =%s,@pAccount =%s,@pHorizontal =%s,@pOpportunity =%s,@pSalesStage =%s,@pDealSize =%s,@pTier =%s,@ReportDt =%s,@WinzoneID =%s,@pOppSource =%s ,@pAccType=%s"

        common_args = (
            vertical,
            market,
            unique_mcu,
            unique_sbu,
            unique_account_name,
            'All',
            opptype,
            salesstage,
            dealsize,
            tierdata,
            idx_label,
            winzone_id,
            opp_Source,
            Acc_Type
        )
        # print(common_args,"2580")
        try:
            cursor=connection.cursor()
            cursor.execute(sp_cl_stmt, ("AVGTCV",) + common_args)
            col_names = [col_desc[0] for col_desc in cursor.description]
            df = DataFrame.from_records(cursor.fetchall(), columns=col_names)
            tableData = df.to_dict(orient="records")
        except:
            tableData=[]
        return HttpResponse(json.dumps({
            "tableData":tableData
        }))


def AjaxCallForQulUnQualtableData(request):
        idx_label = request.GET.get("idx_label")
        unique_mcu = ','.join(request.GET.getlist("unique_mcu[]"))
        unique_sbu = ','.join(request.GET.getlist("unique_sbu[]"))
        opp_Source = ','.join(request.GET.getlist("opp_Source[]"))
        Acc_Type = ','.join(request.GET.getlist("Acc_Type[]"))
        unique_account_name = ','.join(request.GET.getlist("unique_account_name[]"))
        vertical = request.GET.get("vertical")
        salesstage = request.GET.get("salesstage")
        market = request.GET.get("market")
        winzone_id = request.GET.get("winzone_id")
        dealsize = request.GET.get("dealsize")
        opptype = request.GET.get("opptype")
        tierdata = request.GET.get("tierdata")
        # print(salesstage)
        sp_cl_stmt = "Exec [dbo].[sp_EMEADeliveryConnect_Dev] @ptype =%s,@pvertical =%s,@pRegion =%s,@pBU =%s,@pSBU =%s,@pAccount =%s,@pHorizontal =%s,@pOpportunity =%s,@pSalesStage =%s,@pDealSize =%s,@pTier =%s,@ReportDt =%s,@WinzoneID =%s,@pOppSource =%s ,@pAccType=%s"

        common_args = (
            vertical,
            market,
            unique_mcu,
            unique_sbu,
            unique_account_name,
            'All',
            opptype,
            salesstage,
            dealsize,
            tierdata,
            idx_label,
            winzone_id,
            opp_Source,
            Acc_Type
        )
        # print(common_args,"2627")
        try:
            cursor=connection.cursor()
            cursor.execute(sp_cl_stmt, ("QualPipeTable",) + common_args)
            col_names = [col_desc[0] for col_desc in cursor.description]
            df = DataFrame.from_records(cursor.fetchall(), columns=col_names)
            tableData = df.to_dict(orient="records")
        except:
            tableData=[]
        return HttpResponse(json.dumps({
            "tableData":tableData
        }))


def aggregate_stage_data(df, deal_size_buckets=None):
    sales_stages = ['1. Engagement', '2. Shaping', '3. Solutioning', '4. End-Game', '5. Negotiation']
    deal_size_buckets = deal_size_buckets
    stage_data = {
        stage: {
            'total': 0.0,
            'new': 0.0,
            'renewal': 0.0,
            'count': 0,
            'buckets': {b: 0 for b in deal_size_buckets},
            'bucket_tcv': {b: 0.0 for b in deal_size_buckets},
        }
        for stage in sales_stages
    }
    if df.empty:
        return stage_data

    stage_col = next((c for c in ['Sales Stage'] if c in df.columns), None)
    tcv_col   = next((c for c in ['Gross TCV $', 'TCV'] if c in df.columns), None)
    deal_type_col = next((c for c in ['Deal Type'] if c in df.columns), None)
    deal_size_col = next((c for c in ['DealSize', 'Deal_Size'] if c in df.columns), None)
    if stage_col is None:
        return stage_data
    norm_buckets = [str(b).replace('&gt;=', '>=').strip() for b in deal_size_buckets]
    for stage in sales_stages:
        stage_df = df[df[stage_col] == stage]
        if stage_df.empty:
            continue
        if tcv_col and tcv_col in stage_df.columns:
            stage_data[stage]['total'] = float(round(stage_df[tcv_col].sum(), 2))
        if deal_type_col and deal_type_col in stage_df.columns:
            new_df = stage_df[stage_df[deal_type_col] == 'New']
            renewal_df = stage_df[stage_df[deal_type_col] == 'Renewal']
            stage_data[stage]['new'] = float(round(new_df[tcv_col].sum(), 2)) if tcv_col and not new_df.empty else 0.0
            stage_data[stage]['renewal'] = float(round(renewal_df[tcv_col].sum(), 2)) if tcv_col and not renewal_df.empty else 0.0
        else:
            stage_data[stage]['new'] = stage_data[stage]['total']
            stage_data[stage]['renewal'] = 0.0
        stage_data[stage]['count'] = int(len(stage_df))

        if deal_size_col:
            ds_series = stage_df[deal_size_col].astype(str).str.replace('&gt;=', '>=', regex=False).str.strip()
            for raw_bucket, norm_bucket in zip(deal_size_buckets, norm_buckets):
                mask = (ds_series == norm_bucket)
                count = int(mask.sum())
                stage_data[stage]['buckets'][raw_bucket] = count
                if tcv_col:
                    amt = float(round(stage_df.loc[mask, tcv_col].sum(), 2)) if count else 0.0
                    stage_data[stage]['bucket_tcv'][raw_bucket] = amt

    return stage_data


def pipeline_summary(request):
    stage_data_by_region = {}
    '''Pipeline Summary - Funnel visualization showing TCV by Sales Stage with New/Renewal breakdown'''
    index_labels = getIndexLabels()
    idx_label_selected = request.GET.get('idx_label')
    if idx_label_selected == None:
        idx_label_selected = index_labels[0]
    
    values = getDropDownValues(idx_label_selected)
    selected_values = parse_selected_values(request, values[0], values[1], values[7], values[5], index_labels[0])
    
    # Get filter values
    opt_opp_vertical_new = request.GET.getlist('vertical[]')
    if opt_opp_vertical_new == []:
        opt_opp_vertical_new = values[0]
    opt_opp_region_new = request.GET.getlist('unq_region[]')
    if opt_opp_region_new == []:
        opt_opp_region_new = values[1]
    opt_opp_type = request.GET.getlist('type[]')
    if opt_opp_type == []:
        opt_opp_type = values[7]
    opt_mcu_checked = request.GET.getlist('unique_mcu[]')
    if opt_mcu_checked == []:
        opt_mcu_checked = ['All']
    opt_sbu_checked = request.GET.getlist('unique_sbu[]')
    if opt_sbu_checked == []:
        opt_sbu_checked = ['All']
    opt_account_name_checked = request.GET.getlist('unique_account_name[]')
    if opt_account_name_checked == []:
        opt_account_name_checked = ['All']
    opt_horizontal_checked = request.GET.getlist('unique_horizontal[]')
    if opt_horizontal_checked == []:
        opt_horizontal_checked = ['All']
    opt_deal_size = request.GET.getlist('deal_size[]')
    if opt_deal_size == []:
        opt_deal_size = ["$0m - $2.5m", "$2.5m - $10m", "$10m - $25m", "$25m - $50m", ">= $50m"]
    opt_tier_data = request.GET.getlist('tier_data[]')
    if opt_tier_data == []:
        opt_tier_data = values[5]
    
    # Build stored procedure call
    sp_cl_stmt = 'EXEC [dbo].[get_PipelineReport_Dev] @ptype= %s,@pvertical= %s,@pRegion= %s,@pBU= %s,@pSBU= %s,@pAccount= %s,@pHorizontal= %s,@pOpportunity= %s,@pSalesStage= %s,@pDealSize= %s,@pTier= %s,@ReportDt= %s,@WinzoneID=%s'
    
    # Get pipeline data for all active sales stages
    all_sales_stages = '1. Engagement,2. Shaping,3. Solutioning,4. End-Game,5. Negotiation'
    # Force Pipeline Summary to use these 5 stages regardless of filter selection
    implicit_stages = '1. Engagement,2. Shaping,3. Solutioning,4. End-Game,5. Negotiation'

    common_args = (
        selected_values['selected_vertical'],
        selected_values['selected_region'],
        selected_values['selected_bu'],
        selected_values['selected_sbu'],
        selected_values['selected_account'],
        'All',
        selected_values['selected_oppType'],
        implicit_stages, # <--- FIXED: Hardcoded for this page only
        selected_values['selected_dealsize'],
        selected_values['selected_tier'],
        selected_values['idx_label_selected'],
        selected_values['selected_winzoneID']
    )
    
    try:
        cursor = connection.cursor()
        cursor.execute(sp_cl_stmt, ('Pipelinetbl_b',) + common_args)
        col_names = [col_desc[0] for col_desc in cursor.description]
        df = DataFrame.from_records(cursor.fetchall(), columns=col_names)
        
        # Define sales stages in order
        sales_stages = ['1. Engagement', '2. Shaping', '3. Solutioning', '4. End-Game', '5. Negotiation']
        
        # Initialize data structure
        stage_data = {}
        for stage in sales_stages:
            stage_data[stage] = {
                'total': 0,
                'new': 0,
                'renewal': 0,
                'count': 0,
                'new_count': 0,
                'renewal_count': 0
            }
        
        # Aggregate data by Sales Stage and Deal Type (New/Renewal)
        if not df.empty and 'Sales Stage' in df.columns and 'TCV' in df.columns:
            # Determine the deal type column name
            deal_type_col = None
            if 'R_Deal Type' in df.columns:
                deal_type_col = 'R_Deal Type'
            elif 'Type' in df.columns:
                deal_type_col = 'Type'
            elif 'Deal Type' in df.columns:
                deal_type_col = 'Deal Type'
            
            for stage in sales_stages:
                stage_df = df[df['Sales Stage'] == stage]
                
                if not stage_df.empty:
                    # Total TCV for the stage (convert numpy float64 to Python float)
                    stage_data[stage]['total'] = float(round(stage_df['TCV'].sum(), 2))
                    stage_data[stage]['count'] = int(len(stage_df))
                    
                    # Check if deal type column exists
                    if deal_type_col:
                        new_df = stage_df[stage_df[deal_type_col] == 'New']
                        renewal_df = stage_df[stage_df[deal_type_col] == 'Renewal']
                        
                        stage_data[stage]['new'] = float(round(new_df['TCV'].sum(), 2)) if not new_df.empty else 0
                        stage_data[stage]['renewal'] = float(round(renewal_df['TCV'].sum(), 2)) if not renewal_df.empty else 0
                        stage_data[stage]['new_count'] = int(len(new_df)) if not new_df.empty else 0
                        stage_data[stage]['renewal_count'] = int(len(renewal_df)) if not renewal_df.empty else 0
                    else:
                        # If no deal type column, put all in new
                        stage_data[stage]['new'] = stage_data[stage]['total']
                        stage_data[stage]['renewal'] = 0
                        stage_data[stage]['new_count'] = stage_data[stage]['count']
                        stage_data[stage]['renewal_count'] = 0
            
            # Aggregate by region using already-fetched DataFrame (avoids N extra SP calls per region)
            region_col = next((c for c in ['Market Unit', 'Region', 'R_Market Unit'] if c in df.columns), None)
            if region_col:
                for region in opt_opp_region_new:
                    df_region = df[df[region_col] == region]
                    stage_data_by_region[region] = aggregate_stage_data(df_region, deal_size_buckets=opt_deal_size)
        
        # Calculate Qualified vs Unqualified
        # Unqualified: First 2 stages (Engagement, Shaping)
        unqualified_tcv = float(stage_data['1. Engagement']['total'] + stage_data['2. Shaping']['total'])
        unqualified_count = int(stage_data['1. Engagement']['count'] + stage_data['2. Shaping']['count'])
        
        # Qualified: Last 3 stages (Solutioning, End-Game, Negotiation)
        qualified_tcv = float(stage_data['3. Solutioning']['total'] + stage_data['4. End-Game']['total'] + stage_data['5. Negotiation']['total'])
        qualified_count = int(stage_data['3. Solutioning']['count'] + stage_data['4. End-Game']['count'] + stage_data['5. Negotiation']['count'])
        
        # Calculate total and percentages
        total_tcv = float(unqualified_tcv + qualified_tcv)
        total_count = int(unqualified_count + qualified_count)
        qualified_pct = float(round((qualified_tcv / total_tcv * 100), 0)) if total_tcv > 0 else 0
        unqualified_pct = float(round((unqualified_tcv / total_tcv * 100), 0)) if total_tcv > 0 else 0
        
    except Exception as e:
        print(f'Error in pipeline_summary: {e}')
        traceback.print_exc()
        stage_data = {stage: {'total': 0, 'new': 0, 'renewal': 0, 'count': 0, 'new_count': 0, 'renewal_count': 0} for stage in ['1. Engagement', '2. Shaping', '3. Solutioning', '4. End-Game', '5. Negotiation']}
        stage_data_by_region = {}
        qualified_tcv = 0
        unqualified_tcv = 0
        total_tcv = 0
        qualified_pct = 0
        unqualified_pct = 0
        qualified_count = 0
        unqualified_count = 0
        total_count = 0
    
    # Top 20 Deals by Net TCV
    try:
        with connection.cursor() as cursor:
            # Build the filter conditions based on selected values
            vertical_filter = "','".join(selected_values["selected_vertical"].split(','))
            region_filter = "','".join(selected_values["selected_region"].split(','))
            salesstage_filter = "','".join(all_sales_stages.split(','))
            dealsize_filter = "','".join(selected_values["selected_dealsize"].split(','))
            opptype_filter = "','".join(selected_values["selected_oppType"].split(','))
            
            # Build account filter - skip if 'All' is anywhere in the selection
            account_values = [v.strip() for v in selected_values.get('selected_account', 'All').split(',') if v.strip()]
            if account_values and 'All' not in account_values:
                account_filter_sql = "AND [Account Name] IN ('" + "','".join(account_values) + "')"
            else:
                account_filter_sql = ''
            
            # Build BU and SBU filter conditions
            bu_values = [v.strip() for v in selected_values.get('selected_bu', 'All').split(',') if v.strip()]
            if bu_values and 'All' not in bu_values:
                bu_filter_sql = "AND [BU] IN ('" + "','".join(bu_values) + "')"
            else:
                bu_filter_sql = ''
            
            sbu_values = [v.strip() for v in selected_values.get('selected_sbu', 'All').split(',') if v.strip()]
            if sbu_values and 'All' not in sbu_values:
                sbu_filter_sql = "AND [SBU1] IN ('" + "','".join(sbu_values) + "')"
            else:
                sbu_filter_sql = ''
            
            top20_query = f"""
                SELECT TOP 20 
                    [WinZone Opportunity ID],
                    [Opportunity Name],
                    [Account Name],
                    [R_Vertical] as Vertical,
                    [Market Unit],
                    [Sales Stage],
                    CASE WHEN [Qualified] = 1 THEN 'Qualified' ELSE 'Not Qualified' END as [Qualified],
                    [R_Deal Type],
                    [CloseDate] as [Close Date],
                    ROUND(ISNULL([Net TCV], 0)/1000000, 6) as [Net TCV]
                FROM [dbo].[Pipelinedata]
                WHERE uploadedon = '{selected_values['idx_label_selected']}'
                    AND [R_Vertical] IN ('{vertical_filter}')
                    AND [Market Unit] IN ('{region_filter}')
                    AND [Sales Stage] IN ('{salesstage_filter}')
                    AND [DealSize] IN ('{dealsize_filter}')
                    AND [R_Deal Type] IN ('{opptype_filter}')
                    {account_filter_sql}
                    {bu_filter_sql}
                    {sbu_filter_sql}
                ORDER BY [Net TCV] DESC
            """
            cursor.execute(top20_query)
            col_names = [col_desc[0] for col_desc in cursor.description]
            top20Df = DataFrame.from_records(cursor.fetchall(), columns=col_names)
            if not top20Df.empty:
                if 'Close Date' in top20Df.columns:
                    top20Df['Close Date'] = pd.to_datetime(top20Df['Close Date'], errors='coerce').dt.strftime('%m/%d/%Y')
                if 'Vertical' in top20Df.columns:
                    top20Df['Vertical'] = top20Df['Vertical'].fillna('')
            top20DealsData = top20Df.to_dict(orient="records")
    except Exception as e:
        print(f"Error fetching top 20 deals: {e}")
        top20DealsData = []
    
    # Recent Won/Lost Deals - Last 2 weeks data
    try:
        with connection.cursor() as cursor:
            # Build the filter conditions based on selected values
            vertical_filter = "','".join(selected_values["selected_vertical"].split(','))
            region_filter = "','".join(selected_values["selected_region"].split(','))
            dealsize_filter = "','".join(selected_values["selected_dealsize"].split(','))
            opptype_filter = "','".join(selected_values["selected_oppType"].split(','))
            
            # Build BU and SBU filter conditions
            bu_values = [v.strip() for v in selected_values.get('selected_bu', 'All').split(',') if v.strip()]
            if bu_values and 'All' not in bu_values:
                bu_filter_sql = "AND [BU] IN ('" + "','".join(bu_values) + "')"
            else:
                bu_filter_sql = ''
            
            sbu_values = [v.strip() for v in selected_values.get('selected_sbu', 'All').split(',') if v.strip()]
            if sbu_values and 'All' not in sbu_values:
                sbu_filter_sql = "AND [SBU1] IN ('" + "','".join(sbu_values) + "')"
            else:
                sbu_filter_sql = ''
            
            # Determine reference date condition for Won/Lost recent filter
            ref_date_inner = selected_values.get('idx_label_selected')
            if ref_date_inner:
                date_condition_wonlost = f"AND [Last Modified Date] >= DATEADD(WEEK, -2, CONVERT(DATETIME, '{ref_date_inner}', 101))"
            else:
                date_condition_wonlost = "AND [Last Modified Date] >= DATEADD(WEEK, -2, GETDATE())"

            wonlost_query = f"""
                SELECT TOP 20 
                    [WinZone Opportunity ID],
                    [Opportunity Name],
                    [Account Name],
                    [R_Vertical] as Vertical,
                    [Market Unit],
                    [Sales Stage],
                    CASE WHEN [Qualified] = 1 THEN 'Qualified' ELSE 'Not Qualified' END as [Qualified],
                    [Last Modified Date],
                    [CloseDate] as [Close Date],
                    ROUND(ISNULL([Net TCV], 0)/1000000, 6) as [Net TCV]
                FROM [dbo].[Pipelinedata]
                WHERE uploadedon = '{selected_values['idx_label_selected']}'
                    AND [R_Vertical] IN ('{vertical_filter}')
                    AND [Market Unit] IN ('{region_filter}')
                    AND [Sales Stage] IN ('Won', 'Lost')
                    AND [DealSize] IN ('{dealsize_filter}')
                    AND [R_Deal Type] IN ('{opptype_filter}')
                    {"AND [Account Name] IN ('" + "','".join(selected_values['selected_account'].split(',')) + "')" if selected_values.get('selected_account') and 'All' not in selected_values['selected_account'].split(',') else ''}
                    {bu_filter_sql}
                    {sbu_filter_sql}
                    {date_condition_wonlost}
                ORDER BY [Net TCV] DESC
            """
            cursor.execute(wonlost_query)
            col_names = [col_desc[0] for col_desc in cursor.description]
            wonLostDf = DataFrame.from_records(cursor.fetchall(), columns=col_names)
            # Format Last Modified Date to string for JSON serialization
            if not wonLostDf.empty:
                if 'Last Modified Date' in wonLostDf.columns:
                    wonLostDf['Last Modified Date'] = pd.to_datetime(wonLostDf['Last Modified Date']).dt.strftime('%m/%d/%Y')
                if 'Close Date' in wonLostDf.columns:
                    wonLostDf['Close Date'] = pd.to_datetime(wonLostDf['Close Date'], errors='coerce').dt.strftime('%m/%d/%Y')
            wonLostDealsData = wonLostDf.to_dict(orient="records")
    except Exception as e:
        print(f"Error fetching won/lost deals: {e}")
        traceback.print_exc()
        wonLostDealsData = []
    
    # Get Recent Created Deals - Last 2 weeks data (same approach as Won/Lost Deals)
    limit = request.GET.get('limit', '20')
    try:
        limit = int(limit)
    except:
        limit = 20
    
    top_deals = []
    try:
        with connection.cursor() as cursor:
            # Build the filter conditions based on selected values
            vertical_filter = "','".join(selected_values["selected_vertical"].split(','))
            region_filter = "','".join(selected_values["selected_region"].split(','))
            dealsize_filter = "','".join(selected_values["selected_dealsize"].split(','))
            opptype_filter = "','".join(selected_values["selected_oppType"].split(','))
            
            # Build BU and SBU filter conditions
            bu_values = [v.strip() for v in selected_values.get('selected_bu', 'All').split(',') if v.strip()]
            if bu_values and 'All' not in bu_values:
                bu_filter_sql = "AND [BU] IN ('" + "','".join(bu_values) + "')"
            else:
                bu_filter_sql = ''
            
            sbu_values = [v.strip() for v in selected_values.get('selected_sbu', 'All').split(',') if v.strip()]
            if sbu_values and 'All' not in sbu_values:
                sbu_filter_sql = "AND [SBU1] IN ('" + "','".join(sbu_values) + "')"
            else:
                sbu_filter_sql = ''
            
            # Query for Recent Created Deals - same structure as Won/Lost but filters by Created Date
            # Use selected idx_label as reference date for "last 2 weeks"
            ref_date = selected_values.get('idx_label_selected')
            if ref_date:
                # selected idx_label expected in MM/DD/YYYY
                date_condition = f"AND [Created Date] >= DATEADD(WEEK, -2, CONVERT(DATETIME, '{ref_date}', 101))"
            else:
                date_condition = "AND [Created Date] >= DATEADD(WEEK, -2, GETDATE())"

            recent_deals_query = f"""
                SELECT TOP {limit}
                    CAST([WinZone Opportunity ID] AS BIGINT) as [WinZone Opportunity ID],
                    [Opportunity Name],
                    ISNULL([Account Name], '') as [Account Name],
                    [R_Vertical] as Vertical,
                    [Market Unit],
                    [Sales Stage],
                    CASE 
                        WHEN [Sales Stage] IN ('3. Solutioning', '4. End-Game', '5. Negotiation') THEN 'Qualified'
                        ELSE 'Not Qualified'
                    END as [Qualified],
                    [Created Date],
                    [CloseDate] as [Close Date],
                    ROUND(ISNULL([Net TCV], 0)/1000000, 6) as [Net TCV]
                FROM [dbo].[Pipelinedata]
                WHERE uploadedon = '{selected_values['idx_label_selected']}'
                    AND [R_Vertical] IN ('{vertical_filter}')
                    AND [Market Unit] IN ('{region_filter}')
                    AND [Sales Stage] IN ('1. Engagement', '2. Shaping', '3. Solutioning', '4. End-Game', '5. Negotiation')
                    AND [DealSize] IN ('{dealsize_filter}')
                    AND [R_Deal Type] IN ('{opptype_filter}')
                    {"AND [Account Name] IN ('" + "','".join(selected_values['selected_account'].split(',')) + "')" if selected_values.get('selected_account') and 'All' not in selected_values['selected_account'].split(',') else ''}
                    {bu_filter_sql}
                    {sbu_filter_sql}
                    {date_condition}
                ORDER BY [Net TCV] DESC
            """
            cursor.execute(recent_deals_query)
            col_names = [col_desc[0] for col_desc in cursor.description]
            recentDealsDf = DataFrame.from_records(cursor.fetchall(), columns=col_names)
            
            # Format Created Date to string for JSON serialization
            if not recentDealsDf.empty:
                if 'Created Date' in recentDealsDf.columns:
                    recentDealsDf['Created Date'] = pd.to_datetime(recentDealsDf['Created Date']).dt.strftime('%m/%d/%Y')
                if 'Close Date' in recentDealsDf.columns:
                    recentDealsDf['Close Date'] = pd.to_datetime(recentDealsDf['Close Date'], errors='coerce').dt.strftime('%m/%d/%Y')
            
            # Convert to list of dicts with proper key names for template
            if not recentDealsDf.empty:
                top_deals = recentDealsDf.rename(columns={
                    'WinZone Opportunity ID': 'Opportunity_Id',
                    'Opportunity Name': 'Opportunity_Name',
                    'Market Unit': 'Market_Unit',
                    'Sales Stage': 'Sales_Stage',
                    'Qualified': 'Qualified',
                    'Net TCV': 'Net_TCV',
                    'Account Name': 'Account_Name',
                    'Created Date': 'Created_Date',
                    'Vertical': 'Vertical',
                    'Close Date': 'Close_Date'
                }).to_dict(orient='records')
    except Exception as e:
        print(f"Error fetching recent created deals: {e}")
        traceback.print_exc()
        top_deals = []

    # Get Recent Last Modified Deals - Last 2 weeks based on Last Modified Date
    last_modified_deals = []
    last_modified_limit = request.GET.get('last_modified_limit', '20')
    try:
        last_modified_limit = int(last_modified_limit)
    except:
        last_modified_limit = 20
    
    try:
        with connection.cursor() as cursor:
            # Build the filter conditions based on selected values
            vertical_filter = "','".join(selected_values["selected_vertical"].split(','))
            region_filter = "','".join(selected_values["selected_region"].split(','))
            dealsize_filter = "','".join(selected_values["selected_dealsize"].split(','))
            opptype_filter = "','".join(selected_values["selected_oppType"].split(','))
            
            # Build BU and SBU filter conditions
            bu_values = [v.strip() for v in selected_values.get('selected_bu', 'All').split(',') if v.strip()]
            if bu_values and 'All' not in bu_values:
                bu_filter_sql = "AND [BU] IN ('" + "','".join(bu_values) + "')"
            else:
                bu_filter_sql = ''
            
            sbu_values = [v.strip() for v in selected_values.get('selected_sbu', 'All').split(',') if v.strip()]
            if sbu_values and 'All' not in sbu_values:
                sbu_filter_sql = "AND [SBU1] IN ('" + "','".join(sbu_values) + "')"
            else:
                sbu_filter_sql = ''
            
            # Query for Recent Last Modified Deals - filters by Last Modified Date
            # Use selected idx_label as reference date for "last 2 weeks"
            ref_date = selected_values.get('idx_label_selected')
            if ref_date:
                # selected idx_label expected in MM/DD/YYYY
                date_condition_modified = f"AND [Last Modified Date] >= DATEADD(WEEK, -2, CONVERT(DATETIME, '{ref_date}', 101))"
            else:
                date_condition_modified = "AND [Last Modified Date] >= DATEADD(WEEK, -2, GETDATE())"

            last_modified_query = f"""
                SELECT TOP {last_modified_limit}
                    CAST([WinZone Opportunity ID] AS BIGINT) as [WinZone Opportunity ID],
                    [Opportunity Name],
                    ISNULL([Account Name], '') as [Account Name],
                    [R_Vertical] as Vertical,
                    [Market Unit],
                    [Sales Stage],
                    CASE WHEN [Qualified] = 1 THEN 'Qualified' ELSE 'Not Qualified' END as [Qualified],
                    [Last Modified Date],
                    [CloseDate] as [Close Date],
                    ROUND(ISNULL([Net TCV], 0)/1000000, 6) as [Net TCV]
                FROM [dbo].[Pipelinedata]
                WHERE uploadedon = '{selected_values['idx_label_selected']}'
                    AND [R_Vertical] IN ('{vertical_filter}')
                    AND [Market Unit] IN ('{region_filter}')
                    AND [Sales Stage] IN ('1. Engagement', '2. Shaping', '3. Solutioning', '4. End-Game', '5. Negotiation')
                    AND [DealSize] IN ('{dealsize_filter}')
                    AND [R_Deal Type] IN ('{opptype_filter}')
                    {"AND [Account Name] IN ('" + "','".join(selected_values['selected_account'].split(',')) + "')" if selected_values.get('selected_account') and 'All' not in selected_values['selected_account'].split(',') else ''}
                    {bu_filter_sql}
                    {sbu_filter_sql}
                    {date_condition_modified}
                ORDER BY [Net TCV] DESC
            """
            cursor.execute(last_modified_query)
            col_names = [col_desc[0] for col_desc in cursor.description]
            lastModifiedDf = DataFrame.from_records(cursor.fetchall(), columns=col_names)
            
            # Format Last Modified Date to string for JSON serialization
            if not lastModifiedDf.empty:
                if 'Last Modified Date' in lastModifiedDf.columns:
                    lastModifiedDf['Last Modified Date'] = pd.to_datetime(lastModifiedDf['Last Modified Date']).dt.strftime('%m/%d/%Y')
                if 'Close Date' in lastModifiedDf.columns:
                    lastModifiedDf['Close Date'] = pd.to_datetime(lastModifiedDf['Close Date'], errors='coerce').dt.strftime('%m/%d/%Y')
            
            # Convert to list of dicts with proper key names for template
            if not lastModifiedDf.empty:
                last_modified_deals = lastModifiedDf.rename(columns={
                    'WinZone Opportunity ID': 'WinZone_Opportunity_ID',
                    'Opportunity Name': 'Opportunity_Name',
                    'Account Name': 'Account_Name',
                    'Market Unit': 'R_Market_Unit',
                    'Sales Stage': 'Sales_Stage',
                    'Qualified': 'Qualified',
                    'Last Modified Date': 'Last_Modified_Date',
                    'Net TCV': 'Net_TCV',
                    'Vertical': 'Vertical',
                    'Close Date': 'Close_Date'
                }).to_dict(orient='records')
    except Exception as e:
        print(f"Error fetching recent last modified deals: {e}")
        traceback.print_exc()
        last_modified_deals = []

    return render(request, 'visualize/pipeline_summary.html', context={
        'plot_label': 'Pipeline Summary',
        'index_labels': index_labels,
        'idx_label_selected': idx_label_selected,
        'vertical_unique': values[0],
        'region_unique': values[1],
        'unique_mcu': values[2],
        'unique_sbu': values[3],
        'unique_account': values[4],
        'unique_account_name': values[4],
        'unique_horizontal': values[6],
        'opp_type_labels': values[7],
        'opp_type': values[7],
        'sales_stage': values[8],
        'deal_size': ["$0m - $2.5m", "$2.5m - $10m", "$10m - $25m", "$25m - $50m", ">= $50m"],
        'tier_data': values[5],
        'opt_opp_vertical_new': opt_opp_vertical_new,
        'opt_opp_region_new': json.dumps(opt_opp_region_new),
        'opt_opp_type': opt_opp_type,
        'opt_mcu_checked': opt_mcu_checked,
        'opt_sbu_checked': opt_sbu_checked,
        'opt_account_name_checked': opt_account_name_checked,
        'opt_horizontal_checked': opt_horizontal_checked,
        'opt_deal_size': opt_deal_size,
        'opt_sales_stage': [],
        'opt_tier_data': opt_tier_data,
        'selected_bu': selected_values['selected_bu'],
        'selected_sbu': selected_values['selected_sbu'],
        'resultDepData': json.dumps(values[9]),
        'stage_data': json.dumps(stage_data),
        'qualified_tcv': qualified_tcv,
        'unqualified_tcv': unqualified_tcv,
        'total_tcv': total_tcv,
        'qualified_pct': qualified_pct,
        'unqualified_pct': unqualified_pct,
        'qualified_count': qualified_count,
        'unqualified_count': unqualified_count,
        'total_count': total_count,
        'top20DealsData': json.dumps(top20DealsData),
        'wonLostDealsData': json.dumps(wonLostDealsData),
        'top_deals': top_deals,
        'last_modified_deals': last_modified_deals,
        'limit': limit,
        'filter_vertical': selected_values['selected_vertical'],
        'filter_region': selected_values['selected_region'],
        'filter_dealsize': selected_values['selected_dealsize'],
        'filter_opptype': selected_values['selected_oppType'],
        'filter_salesstage': all_sales_stages,
        'filter_account': selected_values['selected_account'],
        'filter_bu': selected_values['selected_bu'],
        'filter_sbu': selected_values['selected_sbu'],
        'stage_data_by_region': json.dumps(stage_data_by_region),
        'deal_size_buckets': json.dumps(opt_deal_size),
    })


def download_top_deals_excel(request):
    '''Download Recent Created Deals as Excel file'''
    from django.http import HttpResponse
    from io import BytesIO
    
    index_labels = getIndexLabels()
    idx_label_selected = request.GET.get('idx_label')
    if idx_label_selected == None:
        idx_label_selected = index_labels[0]
    
    values = getDropDownValues(idx_label_selected)
    selected_values = parse_selected_values(request, values[0], values[1], values[7], values[5], index_labels[0])
    
    limit = request.GET.get('limit', '20')
    try:
        limit = int(limit)
    except:
        limit = 20
    
    try:
        with connection.cursor() as cursor:
            # Build the filter conditions based on selected values
            vertical_filter = "','".join(selected_values["selected_vertical"].split(','))
            region_filter = "','".join(selected_values["selected_region"].split(','))
            dealsize_filter = "','".join(selected_values["selected_dealsize"].split(','))
            opptype_filter = "','".join(selected_values["selected_oppType"].split(','))
            
            # Query for Recent Created Deals - same structure as the main view
            # Use selected idx_label as reference date for "last 2 weeks"
            ref_date = selected_values.get('idx_label_selected')
            if ref_date:
                date_condition = f"AND [Created Date] >= DATEADD(WEEK, -2, CONVERT(DATETIME, '{ref_date}', 101))"
            else:
                date_condition = "AND [Created Date] >= DATEADD(WEEK, -2, GETDATE())"

            recent_deals_query = f"""
                SELECT TOP {limit}
                    CAST([WinZone Opportunity ID] AS BIGINT) as [WinZone Opportunity ID],
                    [Opportunity Name],
                    ISNULL([Account Name], '') as [Account Name],
                    [Market Unit],
                    [Sales Stage],
                    CASE 
                        WHEN [Sales Stage] IN ('3. Solutioning', '4. End-Game', '5. Negotiation') THEN 'Qualified'
                        ELSE 'Not Qualified'
                    END as [Qualified],
                    [Created Date],
                    ROUND(ISNULL([Net TCV], 0)/1000000, 6) as [Net TCV]
                FROM [dbo].[Pipelinedata]
                WHERE uploadedon = '{selected_values['idx_label_selected']}'
                    AND [R_Vertical] IN ('{vertical_filter}')
                    AND [Market Unit] IN ('{region_filter}')
                    AND [Sales Stage] IN ('1. Engagement', '2. Shaping', '3. Solutioning', '4. End-Game', '5. Negotiation')
                    AND [DealSize] IN ('{dealsize_filter}')
                    AND [R_Deal Type] IN ('{opptype_filter}')
                    {date_condition}
                ORDER BY [Created Date] DESC
            """
            cursor.execute(recent_deals_query)
            col_names = [col_desc[0] for col_desc in cursor.description]
            df = pd.DataFrame.from_records(cursor.fetchall(), columns=col_names)
            
            # Format Created Date to string for Excel
            if not df.empty and 'Created Date' in df.columns:
                df['Created Date'] = pd.to_datetime(df['Created Date']).dt.strftime('%m/%d/%Y')
        
        # Create Excel file
        output = BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='Recent Created Deals', index=False)
        
        output.seek(0)
        response = HttpResponse(
            output.read(),
            content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
        response['Content-Disposition'] = f'attachment; filename=Recent_Created_Deals_Top_{limit}.xlsx'
        
        return response
        
    except Exception as e:
        print(f"Error generating Excel: {e}")
        traceback.print_exc()
        return HttpResponse("Error generating Excel file", status=500)
    
def AjaxGetWinzoneDetails(request):
    """Fetches comprehensive details + Demand Matrix + Grade Matrix for a single WinZone ID"""
    winzone_id = request.GET.get("winzone_id", "")
    idx_label_selected = request.GET.get('idx_label')
    if idx_label_selected == None:
        index_labels = getIndexLabels()
        idx_label_selected = index_labels[0] if index_labels else ''
        
    if not winzone_id:
        return JsonResponse({"success": False, "error": "No WinZone ID provided"})

    try:
        with connection.cursor() as cursor:
            # --- QUERY 1: Main Deal Details ---
            query_main = "SELECT TOP 1 * FROM [dbo].[Pipelinedata] WHERE [WinZone Opportunity ID] = %s AND [UploadedOn] = %s"
            cursor.execute(query_main, [winzone_id, idx_label_selected])
            columns = [col[0] for col in cursor.description]
            row = cursor.fetchone()

            if row:
                raw_data = dict(zip(columns, row))
                requested_columns = [
                    "WinZone Opportunity ID", "Opportunity Name", "BU", "R_BU", "OwnerEmpID",
                    "Pursuit Lead", "Vertical", "R_Vertical", "Sub Vertical", "R_Subvertical",
                    "SBU1", "Project Category", "SBU Grouped", "Ultimate Parent Name", "Customer ID",
                    "Account Name", "Account Tagging", "Sales Stage", "SBU2", "Opportunity Record Type",
                    "Opportunity Record Type Name", "Account Type", "Account Record Type", "Scope",
                    "Status", "Proactive Engagement", "Market Unit", "R_Market Unit",
                    "Booking Forecast Category", "Forecast Category", "Qualified", "Deal Type",
                    "R_Deal Type", "Deal Duration (Months)", "Customer Category", "Confidence %",
                    "Win Probability (%)", "Gross TCV $", "Expected Revenue Currency", "Expected Revenue",
                    "Total Horizontal TCV Currency", "Total Horizontal TCV", "Net TCV Currency",
                    "Net TCV", "RevenueMoM", "Current Year Revenue Currency", "Current Year Revenue",
                    "Next Year Revenue Forecast", "Current Year Revenue Forecast",
                    "Next Year Revenue Currency", "Next Year Revenue", "Next Year Revenue (converted) Currency",
                    "Next Year Revenue (converted)", "Days in Stage", "Competitors", "Strategic Account",
                    "Expected Revenue Start Date", "Estimated Deal Close Date", "Actual Close Date",
                    "Created Date", "Last Modified Date", "User Comments", "Risk Category",
                    "Client Partner Id Name", "Client Partner Email", "Age", "Partner Supported",
                    "Partner", "Partnership Code", "Partner Classification", "Strategic Partner",
                    "Strategic Partner Involved?", "Industry", "Industry Segment Id", "Industry Segment",
                    "Customer Profitability Amount Currency", "Customer Profitability Amount",
                    "Customer Profitability (%)", "Practice", "# Service Lines", "Billability Type",
                    "Framework Total Value Currency", "Framework Total Value", "Framework Duration (Months)",
                    "Commercial Model", "Campaign Theme", "Service Delivery Model", "9 Box",
                    "ASK Response Completed?", "3rd Party Advisor (TPA)", "Advisor Company: Account Name",
                    "Sub-Status", "Opportunity Source", "Booking Quarter", "Program Type", "CY Q1 $",
                    "CY Q2 $", "CY Q3 $", "CY Q4 $", "Opportunity: CY REVENUE $",
                    "Forecast First Year Amount Currency", "Forecast First Year Amount",
                    "InceFrstYearAmt Currency", "InceFrstYearAmt", "Forecast: Forecast Name",
                    "CloseDate", "CloseQtr", "CloseMonth", "DealSize", "UploadedOn"
                ]
                
                filtered_data = {}
                for col in requested_columns:
                    val = raw_data.get(col, "-")
                    if pd.notna(val) and hasattr(val, 'strftime'):
                        val = val.strftime('%m/%d/%Y')
                    elif pd.isna(val) or val is None:
                        val = "-"
                    filtered_data[col] = val

                # --- QUERY 2: Demand Matrix (Dates) ---
                query_demand = """
                    SELECT d.Tower, d.[Requirement Start Date], COUNT(d.[Unique ID]) as DemandCount
                    FROM [dbo].[demand] d
                    WHERE d.[Opportunity ID] = %s
                    GROUP BY d.Tower, d.[Requirement Start Date]
                    ORDER BY d.Tower, d.[Requirement Start Date]
                """
                cursor.execute(query_demand, [winzone_id])
                demand_rows = cursor.fetchall()
                
                demand_matrix = []
                for dr in demand_rows:
                    tower = dr[0] if dr[0] else "Unknown"
                    r_date = dr[1]
                    r_date_str = r_date.strftime('%d-%b-%Y') if (r_date and hasattr(r_date, 'strftime')) else (str(r_date) if r_date else "No Date")
                    demand_matrix.append({ "Tower": tower, "StartDate": r_date_str, "Count": dr[2] })

                # --- QUERY 3: Grade Matrix Data ---
                query_grade = """
                    SELECT d.Tower, d.[SO Grade], COUNT(d.[Unique ID]) as GradeCount
                    FROM [dbo].[demand] d
                    WHERE d.[Opportunity ID] = %s
                    GROUP BY d.Tower, d.[SO Grade]
                """
                cursor.execute(query_grade, [winzone_id])
                grade_rows = cursor.fetchall()

                # Normalization Logic
                pa_group = {'PAT', 'PA', 'PT', 'Admin Staff', 'P'}
                d_group = {'D', 'Sr. Dir.', 'Sr. Dir', 'AVP'}
                
                def normalize_grade(g):
                    if not g: return "Unknown"
                    g_str = str(g).strip()
                    if g_str in pa_group: return "PA-"
                    if g_str in d_group: return "D+"
                    return g_str

                grade_counts = {} 
                for gr in grade_rows:
                    tower = gr[0] if gr[0] else "Unknown"
                    norm_grade = normalize_grade(gr[1])
                    key = (tower, norm_grade)
                    grade_counts[key] = grade_counts.get(key, 0) + gr[2]

                grade_matrix = []
                for (t, g), c in grade_counts.items():
                    grade_matrix.append({ "Tower": t, "Grade": g, "Count": c })

                # Master Header List (All Grades - for column ordering only)
                cursor.execute("SELECT DISTINCT [SO Grade] FROM [dbo].[demand] WHERE [SO Grade] IS NOT NULL")
                all_raw_grades = [row[0] for row in cursor.fetchall()]
                unique_normalized = set()
                for rg in all_raw_grades:
                    unique_normalized.add(normalize_grade(rg))
                
                priority_order = ['PA-', 'A', 'SA', 'M', 'SM', 'AD', 'D+', 'AVP', 'Cont']
                final_grade_headers = [g for g in priority_order if g in unique_normalized]
                others = sorted([g for g in unique_normalized if g not in priority_order])
                all_grades = final_grade_headers + others

                return JsonResponse({
                    "success": True, 
                    "data": filtered_data, 
                    "demand_matrix": demand_matrix,
                    "grade_matrix": grade_matrix,
                    "all_grades": all_grades
                })
            else:
                return JsonResponse({"success": False, "error": "Deal not found"})

    except Exception as e:
        print(f"Error fetching Winzone details: {e}")
        return JsonResponse({"success": False, "error": str(e)})
# def pipeline_vs_demand(request):
#     """Pipeline vs Demand page - renders the page shell with filters. Data loaded via AJAX."""
#     index_labels = getIndexLabels()
#     idx_label_selected = request.GET.get('idx_label')
#     if idx_label_selected is None:
#         idx_label_selected = index_labels[0]

#     values = getDropDownValues(idx_label_selected)
#     selected_values = parse_selected_values(request, values[0], values[1], values[7], values[5], index_labels[0])

#     opt_opp_vertical_new = request.GET.getlist('vertical[]') or values[0]
#     opt_opp_region_new = request.GET.getlist('unq_region[]') or values[1]
#     opt_opp_type = request.GET.getlist('type[]') or values[7]
#     opt_mcu_checked = request.GET.getlist('unique_mcu[]') or ['All']
#     opt_sbu_checked = request.GET.getlist('unique_sbu[]') or ['All']
#     opt_account_name_checked = request.GET.getlist('unique_account_name[]') or ['All']
#     opt_horizontal_checked = request.GET.getlist('unique_horizontal[]') or ['All']
#     opt_deal_size = request.GET.getlist('deal_size[]') or ["$0m - $2.5m", "$2.5m - $10m", "$10m - $25m", "$25m - $50m", ">= $50m"]
#     opt_tier_data = request.GET.getlist('tier_data[]') or values[5]

#     return render(request, 'visualize/pipeline_vs_demand.html', context={
#         'plot_label': 'Pipeline vs Demand',
#         'index_labels': index_labels,
#         'idx_label_selected': idx_label_selected,
#         'vertical_unique': values[0],
#         'region_unique': values[1],
#         'unique_mcu': values[2],
#         'unique_sbu': values[3],
#         'unique_account': values[4],
#         'unique_account_name': values[4],
#         'unique_horizontal': values[6],
#         'opp_type_labels': values[7],
#         'opp_type': values[7],
#         'sales_stage': values[8],
#         'deal_size': ["$0m - $2.5m", "$2.5m - $10m", "$10m - $25m", "$25m - $50m", ">= $50m"],
#         'tier_data': values[5],
#         'opt_opp_vertical_new': opt_opp_vertical_new,
#         'opt_opp_region_new': json.dumps(opt_opp_region_new),
#         'opt_opp_type': opt_opp_type,
#         'opt_mcu_checked': opt_mcu_checked,
#         'opt_sbu_checked': opt_sbu_checked,
#         'opt_account_name_checked': opt_account_name_checked,
#         'opt_horizontal_checked': opt_horizontal_checked,
#         'opt_deal_size': opt_deal_size,
#         'opt_sales_stage': [],
#         'opt_tier_data': opt_tier_data,
#         'selected_bu': selected_values['selected_bu'],
#         'selected_sbu': selected_values['selected_sbu'],
#         'resultDepData': json.dumps(values[9]),
#     })


DEFAULT_ACTIVE_STAGES = ['1. Engagement','2. Shaping','3. Solutioning','4. End-Game','5. Negotiation']
DEFAULT_DEAL_SIZES = ["$0m - $2.5m", "$2.5m - $10m", "$10m - $25m", "$25m - $50m", ">= $50m"]
QUALIFIED_STAGES = ['3. Solutioning','4. End-Game','5. Negotiation']
SO_STATUS_OPTIONS = ['OPEN'] #, 'EXTERNAL FULFILMENT', 'INTERNAL FULFILMENT'

def _csv_to_list(csv_str):
    if not csv_str:
        return []
    return [x.strip() for x in csv_str.split(",") if x.strip()]

def _build_in_clause(field, values):
    """
    Returns: (sql_snippet, params_list)
    Example: ("p.[BU] IN (%s,%s)", ["A","B"])
    """
    if not values:
        return "", []
    placeholders = ",".join(["%s"] * len(values))
    return f"{field} IN ({placeholders})", values

def _build_pipeline_filters(request, idx_label_selected):
    """
    Uses same default logic as the page filter system.
    Returns dict of list filters for SQL IN clauses.
    """
    values = getDropDownValues(idx_label_selected)
    selected_values = parse_selected_values(request, values[0], values[1], values[7], values[5], idx_label_selected)

    filters = {
        "verticals": _csv_to_list(selected_values["selected_vertical"]),
        "regions": _csv_to_list(selected_values["selected_region"]),
        "salesstages": _csv_to_list(selected_values["selected_salesstage"]) or DEFAULT_ACTIVE_STAGES,
        "dealsizes": _csv_to_list(selected_values["selected_dealsize"]) or DEFAULT_DEAL_SIZES,
        "opptypes": _csv_to_list(selected_values["selected_oppType"]),
        "bus": _csv_to_list(selected_values["selected_bu"]),
        "sbus": _csv_to_list(selected_values["selected_sbu"]),
        "accounts": _csv_to_list(selected_values["selected_account"]),
        "so_statuses": request.GET.getlist("so_status[]") or SO_STATUS_OPTIONS,
    }
    return filters

def _append_optional_filter(where_parts, params, field, values, skip_if_all=True):
    if not values:
        return
    if skip_if_all and "All" in values:
        return
    clause, p = _build_in_clause(field, values)
    if clause:
        where_parts.append(clause)
        params.extend(p)

def pipeline_vs_demand(request):
    """
    Pipeline vs Demand page - renders the page shell with filters.
    Tables/Chart load data via AJAX endpoints which also apply the same filters.
    """
    index_labels = getIndexLabels()
    idx_label_selected = request.GET.get("idx_label") or (index_labels[0] if index_labels else "")

    # dropdown option lists from DB
    values = getDropDownValues(idx_label_selected)

    # unified parsing (defaults + allowed market units logic)
    selected_values = parse_selected_values(request, values[0], values[1], values[7], values[5], index_labels[0])

    # UI state (selected options) – used by template to keep checks/selected on reload
    opt_opp_vertical_new = request.GET.getlist("vertical[]") or values[0]
    opt_opp_region_new = request.GET.getlist("unq_region[]") or values[1]

    stages = request.GET.getlist("sales_stage[]")
    cleaned = [s for s in stages if s != "Duplicate"]
    opt_sales_stage = cleaned or ['1. Engagement','2. Shaping','3. Solutioning','4. End-Game','5. Negotiation']

    opt_deal_size = request.GET.getlist("deal_size[]") or ["$0m - $2.5m", "$2.5m - $10m", "$10m - $25m", "$25m - $50m", ">= $50m"]
    opt_tier_data = request.GET.getlist("tier_data[]") or values[5]
    opt_opp_type = request.GET.getlist("type[]") or values[7]

    opt_mcu_checked = request.GET.getlist("unique_mcu[]") or ["All"]
    opt_sbu_checked = request.GET.getlist("unique_sbu[]") or ["All"]
    opt_account_name_checked = request.GET.getlist("unique_account_name[]") or ["All"]
    opt_horizontal_checked = request.GET.getlist("unique_horizontal[]") or ["All"]

    
    with connection.cursor() as cursor:
            cursor.execute("""
                SELECT DISTINCT d.Tower
                FROM dbo.demand AS d
                WHERE d.Tower IS NOT NULL
                ORDER BY d.Tower
            """)
            rows = cursor.fetchall()
    towers = [r[0].strip() for r in rows if r[0]]
    towers_json = json.dumps(towers)


    return render(request, "visualize/pipeline_vs_demand.html", context={
        "plot_label": "Pipeline vs Demand",
        "index_labels": index_labels,
        "idx_label_selected": idx_label_selected,

        # option lists
        "vertical_unique": values[0],
        "region_unique": values[1],
        "unique_mcu": values[2],
        "unique_sbu": values[3],
        "unique_account_name": values[4],
        "unique_horizontal": values[6],
        "sales_stage": values[8],
        "opp_type": values[7],
        "deal_size": ["$0m - $2.5m", "$2.5m - $10m", "$10m - $25m", "$25m - $50m", ">= $50m"],
        "tier_data": values[5],
        "resultDepData": json.dumps(values[9]),

        # selected UI values
        "opt_opp_vertical_new": opt_opp_vertical_new,
        "opt_opp_region_new": opt_opp_region_new,
        "opt_opp_region_new_filter": json.dumps(opt_opp_region_new),
        "opt_sales_stage": opt_sales_stage,
        "opt_deal_size": opt_deal_size,
        "opt_tier_data": opt_tier_data,
        "opt_opp_type": opt_opp_type,
        "opt_mcu_checked": opt_mcu_checked,
        "opt_sbu_checked": opt_sbu_checked,
        "opt_account_name_checked": opt_account_name_checked,
        "opt_horizontal_checked": opt_horizontal_checked,

        # used by dependent dropdown js
        "selected_bu": selected_values["selected_bu"],
        "selected_sbu": selected_values["selected_sbu"],

        "towers_json": towers_json,

        # recommended to keep rest consistent
        **selected_values,
    })


def AjaxCallForPipelineVsDemand(request):
    """AJAX endpoint: returns Pipeline vs Demand data as JSON, filtered by drawer selections."""
    try:
        idx_label = request.GET.get("idx_label")
        limit = request.GET.get("limit", "20")

        if not idx_label:
            index_labels = getIndexLabels()
            idx_label = index_labels[0] if index_labels else None
        if not idx_label:
            return JsonResponse({"success": False, "error": "No upload date available"})

        # Convert MM/DD/YYYY to YYYY-MM-DD for SQL
        try:
            dt = datetime.strptime(idx_label, "%m/%d/%Y")
            sql_date = dt.strftime("%Y-%m-%d")
        except Exception:
            sql_date = idx_label

        # TOP clause
        top_clause = ""
        if limit and str(limit).lower() != "all":
            try:
                top_clause = f"TOP {int(limit)}"
            except ValueError:
                top_clause = "TOP 20"

        # --- NEW: build filters from drawer selections ---
        f = _build_pipeline_filters(request, idx_label)

        where_parts = ["p.UploadedOn = %s"]
        params = [sql_date]

        _append_optional_filter(where_parts, params, "p.[R_Vertical]", f["verticals"], skip_if_all=False)
        _append_optional_filter(where_parts, params, "p.[Market Unit]", f["regions"], skip_if_all=False)
        _append_optional_filter(where_parts, params, "p.[Sales Stage]", f["salesstages"])
        _append_optional_filter(where_parts, params, "p.[DealSize]", f["dealsizes"], skip_if_all=False)
        _append_optional_filter(where_parts, params, "p.[R_Deal Type]", f["opptypes"], skip_if_all=False)
        _append_optional_filter(where_parts, params, "d.[SO Line Status]", f["so_statuses"], skip_if_all=False)

        # optional (All means no filter)
        _append_optional_filter(where_parts, params, "p.[BU]", f["bus"])
        _append_optional_filter(where_parts, params, "p.[SBU1]", f["sbus"])
        _append_optional_filter(where_parts, params, "p.[Account Name]", f["accounts"])

        where_sql = " AND ".join(where_parts)

        query = f"""
            SELECT {top_clause}
                d.[Opportunity ID],
                p.[Opportunity Name],
                p.[R_Vertical] AS Vertical,
                p.[Gross TCV $],
                p.CloseDate,
                p.[Sales Stage],
                p.[Account Name],

                COUNT(DISTINCT CASE WHEN d.Tower = N'ADM'                       THEN d.[Unique ID] END) AS [ADM],
                COUNT(DISTINCT CASE WHEN d.Tower = N'AIA'                       THEN d.[Unique ID] END) AS [AIA],
                COUNT(DISTINCT CASE WHEN d.Tower = N'Business Support'          THEN d.[Unique ID] END) AS [Business Support],
                COUNT(DISTINCT CASE WHEN d.Tower = N'CIS'                       THEN d.[Unique ID] END) AS [CIS],
                COUNT(DISTINCT CASE WHEN d.Tower = N'Cognizant Moment'          THEN d.[Unique ID] END) AS [Cognizant Moment],
                COUNT(DISTINCT CASE WHEN d.Tower = N'Consulting'                THEN d.[Unique ID] END) AS [Consulting],
                COUNT(DISTINCT CASE WHEN d.Tower = N'Corporate'                 THEN d.[Unique ID] END) AS [Corporate],
                COUNT(DISTINCT CASE WHEN d.Tower = N'Cybersecurity'             THEN d.[Unique ID] END) AS [Cybersecurity],
                COUNT(DISTINCT CASE WHEN d.Tower = N'Digital Engineering'       THEN d.[Unique ID] END) AS [Digital Engineering],
                COUNT(DISTINCT CASE WHEN d.Tower = N'EPS'                       THEN d.[Unique ID] END) AS [EPS],
                COUNT(DISTINCT CASE WHEN d.Tower = N'Industry Solutions Group'  THEN d.[Unique ID] END) AS [Industry Solutions Group],
                COUNT(DISTINCT CASE WHEN d.Tower = N'IOA'                       THEN d.[Unique ID] END) AS [IOA],
                COUNT(DISTINCT CASE WHEN d.Tower = N'IoT'                       THEN d.[Unique ID] END) AS [IoT],
                COUNT(DISTINCT CASE WHEN d.Tower = N'MDU'                       THEN d.[Unique ID] END) AS [MDU],
                COUNT(DISTINCT CASE WHEN d.Tower = N'Others'                    THEN d.[Unique ID] END) AS [Others],
                COUNT(DISTINCT CASE WHEN d.Tower = N'QEA'                       THEN d.[Unique ID] END) AS [QEA],
                COUNT(DISTINCT CASE WHEN d.Tower = N'ServiceNow Business Group' THEN d.[Unique ID] END) AS [ServiceNow Business Group],

                COUNT(DISTINCT d.[Unique ID]) AS [Demand Total]
            FROM dbo.Pipelinedata AS p
            INNER JOIN dbo.demand AS d
                ON p.[WinZone Opportunity ID] = d.[Opportunity ID]
            WHERE {where_sql}
            GROUP BY
                d.[Opportunity ID],
                p.[Opportunity Name],
                p.[R_Vertical],
                p.[Gross TCV $],
                p.CloseDate,
                p.[Sales Stage],
                p.[Account Name]
            ORDER BY p.[Gross TCV $] DESC
        """

        with connection.cursor() as cursor:
            cursor.execute(query, params)
            col_names = [col[0] for col in cursor.description]
            rows = cursor.fetchall()

        data = []
        for row in rows:
            record = {}
            for i, col in enumerate(col_names):
                val = row[i]
                if val is None:
                    record[col] = ""
                elif hasattr(val, "strftime"):
                    record[col] = val.strftime("%m/%d/%Y")
                elif isinstance(val, (int, float, numpy.integer, numpy.floating)):
                    record[col] = float(val) if isinstance(val, (float, numpy.floating)) else int(val)
                else:
                    record[col] = str(val)
            data.append(record)

        return JsonResponse({"success": True, "data": data})

    except Exception as e:
        print(f"Error in AjaxCallForPipelineVsDemand: {e}")
        traceback.print_exc()
        return JsonResponse({"success": False, "error": str(e)})


def AjaxCallForPVDStats(request):
    """Summary stats banner for Pipeline vs Demand: opportunity count, total TCV (in $m),
    and total demands — never uses a TOP/LIMIT clause so the banner always reflects
    the full filtered dataset regardless of the row-limit dropdown."""
    try:
        idx_label = request.GET.get("idx_label")

        if not idx_label:
            index_labels = getIndexLabels()
            idx_label = index_labels[0] if index_labels else None
        if not idx_label:
            return JsonResponse({"success": False, "error": "No upload date available"})

        try:
            dt = datetime.strptime(idx_label, "%m/%d/%Y")
            sql_date = dt.strftime("%Y-%m-%d")
        except Exception:
            sql_date = idx_label

        f = _build_pipeline_filters(request, idx_label)

        where_parts = ["p.UploadedOn = %s"]
        params = [sql_date]

        _append_optional_filter(where_parts, params, "p.[R_Vertical]", f["verticals"], skip_if_all=False)
        _append_optional_filter(where_parts, params, "p.[Market Unit]", f["regions"], skip_if_all=False)
        _append_optional_filter(where_parts, params, "p.[Sales Stage]", f["salesstages"])
        _append_optional_filter(where_parts, params, "p.[DealSize]", f["dealsizes"], skip_if_all=False)
        _append_optional_filter(where_parts, params, "p.[R_Deal Type]", f["opptypes"], skip_if_all=False)
        _append_optional_filter(where_parts, params, "d.[SO Line Status]", f["so_statuses"], skip_if_all=False)
        towers = request.GET.getlist("towers[]")
        _append_optional_filter(where_parts, params, "d.[Tower]", towers, skip_if_all=False)

        _append_optional_filter(where_parts, params, "p.[BU]", f["bus"])
        _append_optional_filter(where_parts, params, "p.[SBU1]", f["sbus"])
        _append_optional_filter(where_parts, params, "p.[Account Name]", f["accounts"])

        where_sql = " AND ".join(where_parts)

        # Aggregate per opportunity first (avoids double-counting TCV when the same
        # opportunity joins to multiple demand rows), then sum across all opps.
        query = f"""
            SELECT
                COUNT(DISTINCT sub.[Opportunity ID])     AS [Opp Count],
                SUM([Gross TCV $])          AS [Total TCV],
                SUM([Total Demands])        AS [Total Demands]
            FROM (
                SELECT
                    d.[Opportunity ID] AS [Opportunity ID],
                    SUM(p.[Gross TCV $])            AS [Gross TCV $],
                    COUNT(DISTINCT d.[Unique ID])   AS [Total Demands]
                FROM dbo.Pipelinedata AS p
                INNER JOIN dbo.demand AS d
                    ON p.[WinZone Opportunity ID] = d.[Opportunity ID]
                WHERE {where_sql}
                GROUP BY d.[Opportunity ID]
            ) AS sub
        """

        with connection.cursor() as cursor:
            cursor.execute(query, params)
            row = cursor.fetchone()

        opp_count     = int(row[0])   if row and row[0] is not None else 0
        total_tcv     = float(row[1]) if row and row[1] is not None else 0.0
        total_demands = int(row[2])   if row and row[2] is not None else 0

        return JsonResponse({
            "success":        True,
            "opp_count":      opp_count,
            "total_tcv_m":    round(total_tcv / 1_000_000, 2),
            "total_demands":  total_demands,
        })

    except Exception as e:
        print(f"Error in AjaxCallForPVDStats: {e}")
        traceback.print_exc()
        return JsonResponse({"success": False, "error": str(e)})


def AjaxCallForQualifiedPipelineZeroDemand(request):
    """Qualified pipeline (3-5) deals that have zero demands, filtered by drawer selections."""
    try:
        idx_label = request.GET.get("idx_label")
        limit = request.GET.get("limit", "20")

        if not idx_label:
            index_labels = getIndexLabels()
            idx_label = index_labels[0] if index_labels else None
        if not idx_label:
            return JsonResponse({"success": False, "error": "No upload date available"})

        try:
            dt = datetime.strptime(idx_label, "%m/%d/%Y")
            sql_date = dt.strftime("%Y-%m-%d")
        except Exception:
            sql_date = idx_label

        top_clause = ""
        if limit and str(limit).lower() != "all":
            try:
                top_clause = f"TOP {int(limit)}"
            except ValueError:
                top_clause = "TOP 20"

        f = _build_pipeline_filters(request, idx_label)

        # restrict to qualified stages (3-5)
        selected = set(f["salesstages"] or QUALIFIED_STAGES)
        effective_salesstages = sorted(list(selected.intersection(set(QUALIFIED_STAGES)))) or QUALIFIED_STAGES

        where_parts = ["p.UploadedOn = %s"]
        params = [sql_date]

        _append_optional_filter(where_parts, params, "p.[R_Vertical]", f["verticals"], skip_if_all=False)
        _append_optional_filter(where_parts, params, "p.[Market Unit]", f["regions"], skip_if_all=False)
        _append_optional_filter(where_parts, params, "p.[Sales Stage]", effective_salesstages, skip_if_all=False)
        _append_optional_filter(where_parts, params, "p.[DealSize]", f["dealsizes"], skip_if_all=False)
        _append_optional_filter(where_parts, params, "p.[R_Deal Type]", f["opptypes"], skip_if_all=False)

        _append_optional_filter(where_parts, params, "p.[BU]", f["bus"])
        _append_optional_filter(where_parts, params, "p.[SBU1]", f["sbus"])
        _append_optional_filter(where_parts, params, "p.[Account Name]", f["accounts"])

        where_sql = " AND ".join(where_parts)

        query = f"""
            SELECT {top_clause}
                p.[WinZone Opportunity ID] AS [Opportunity ID],
                p.[Opportunity Name],
                p.[Account Name],
                p.[R_Vertical] AS Vertical,
                p.[Sales Stage],
                p.CloseDate,
                p.[Gross TCV $] AS [Net TCV]
            FROM dbo.Pipelinedata AS p
            WHERE {where_sql}
              AND NOT EXISTS (
                  SELECT 1
                  FROM dbo.demand AS d
                  WHERE d.[Opportunity ID] = p.[WinZone Opportunity ID]
              )
            ORDER BY p.[Gross TCV $] DESC
        """

        with connection.cursor() as cursor:
            cursor.execute(query, params)
            col_names = [col[0] for col in cursor.description]
            rows = cursor.fetchall()

        data = []
        for row in rows:
            rec = {}
            for i, col in enumerate(col_names):
                val = row[i]
                if val is None:
                    rec[col] = ""
                elif hasattr(val, "strftime"):
                    rec[col] = val.strftime("%m/%d/%Y")
                else:
                    rec[col] = str(val)
            data.append(rec)

        return JsonResponse({"success": True, "data": data})

    except Exception as e:
        print(f"Error in AjaxCallForQualifiedPipelineZeroDemand: {e}")
        traceback.print_exc()
        return JsonResponse({"success": False, "error": str(e)})


def AjaxCallForDemandsChart(request):
    """Demands chart data endpoint - returns tower counts per opportunity with filters applied."""
    try:
        idx_label = request.GET.get("idx_label")
        limit = request.GET.get("limit", "all")

        if not idx_label:
            index_labels = getIndexLabels()
            idx_label = index_labels[0] if index_labels else None
        if not idx_label:
            return JsonResponse({"success": False, "error": "No upload date available"})

        try:
            dt = datetime.strptime(idx_label, "%m/%d/%Y")
            sql_date = dt.strftime("%Y-%m-%d")
        except Exception:
            sql_date = idx_label

        top_clause = ""
        if limit and str(limit).lower() != "all":
            try:
                top_clause = f"TOP {int(limit)}"
            except ValueError:
                top_clause = ""

        f = _build_pipeline_filters(request, idx_label)

        where_parts = ["p.UploadedOn = %s"]
        params = [sql_date]

        _append_optional_filter(where_parts, params, "p.[R_Vertical]", f["verticals"], skip_if_all=False)
        _append_optional_filter(where_parts, params, "p.[Market Unit]", f["regions"], skip_if_all=False)
        _append_optional_filter(where_parts, params, "p.[Sales Stage]", f["salesstages"])
        _append_optional_filter(where_parts, params, "p.[DealSize]", f["dealsizes"], skip_if_all=False)
        _append_optional_filter(where_parts, params, "p.[R_Deal Type]", f["opptypes"], skip_if_all=False)
        _append_optional_filter(where_parts, params, "d.[SO Line Status]", f["so_statuses"], skip_if_all=False)

        _append_optional_filter(where_parts, params, "p.[BU]", f["bus"])
        _append_optional_filter(where_parts, params, "p.[SBU1]", f["sbus"])
        _append_optional_filter(where_parts, params, "p.[Account Name]", f["accounts"])

        where_sql = " AND ".join(where_parts)

        query = f"""
            SELECT {top_clause}
                d.[Opportunity ID],
                p.[Opportunity Name],
                p.[R_Vertical] AS Vertical,
                p.[Account Name],
                p.[Sales Stage],
                p.CloseDate,

                COUNT(DISTINCT CASE WHEN d.Tower = N'ADM'                       THEN d.[Unique ID] END) AS [ADM],
                COUNT(DISTINCT CASE WHEN d.Tower = N'AIA'                       THEN d.[Unique ID] END) AS [AIA],
                COUNT(DISTINCT CASE WHEN d.Tower = N'Business Support'          THEN d.[Unique ID] END) AS [Business Support],
                COUNT(DISTINCT CASE WHEN d.Tower = N'CIS'                       THEN d.[Unique ID] END) AS [CIS],
                COUNT(DISTINCT CASE WHEN d.Tower = N'Cognizant Moment'          THEN d.[Unique ID] END) AS [Cognizant Moment],
                COUNT(DISTINCT CASE WHEN d.Tower = N'Consulting'                THEN d.[Unique ID] END) AS [Consulting],
                COUNT(DISTINCT CASE WHEN d.Tower = N'Corporate'                 THEN d.[Unique ID] END) AS [Corporate],
                COUNT(DISTINCT CASE WHEN d.Tower = N'Cybersecurity'             THEN d.[Unique ID] END) AS [Cybersecurity],
                COUNT(DISTINCT CASE WHEN d.Tower = N'Digital Engineering'       THEN d.[Unique ID] END) AS [Digital Engineering],
                COUNT(DISTINCT CASE WHEN d.Tower = N'EPS'                       THEN d.[Unique ID] END) AS [EPS],
                COUNT(DISTINCT CASE WHEN d.Tower = N'Industry Solutions Group'  THEN d.[Unique ID] END) AS [Industry Solutions Group],
                COUNT(DISTINCT CASE WHEN d.Tower = N'IOA'                       THEN d.[Unique ID] END) AS [IOA],
                COUNT(DISTINCT CASE WHEN d.Tower = N'IoT'                       THEN d.[Unique ID] END) AS [IoT],
                COUNT(DISTINCT CASE WHEN d.Tower = N'MDU'                       THEN d.[Unique ID] END) AS [MDU],
                COUNT(DISTINCT CASE WHEN d.Tower = N'Others'                    THEN d.[Unique ID] END) AS [Others],
                COUNT(DISTINCT CASE WHEN d.Tower = N'QEA'                       THEN d.[Unique ID] END) AS [QEA],
                COUNT(DISTINCT CASE WHEN d.Tower = N'ServiceNow Business Group' THEN d.[Unique ID] END) AS [ServiceNow Business Group],

                COUNT(DISTINCT d.[Unique ID]) AS [Demand Total]
            FROM dbo.Pipelinedata AS p
            INNER JOIN dbo.demand AS d
                ON p.[WinZone Opportunity ID] = d.[Opportunity ID]
            WHERE {where_sql}
            GROUP BY
                d.[Opportunity ID],
                p.[Opportunity Name],
                p.[R_Vertical],
                p.[Account Name],
                p.[Sales Stage],
                p.CloseDate
            ORDER BY p.CloseDate
        """

        with connection.cursor() as cursor:
            cursor.execute(query, params)
            col_names = [col[0] for col in cursor.description]
            rows = cursor.fetchall()

        out = []
        for row in rows:
            rec = {}
            for i, col in enumerate(col_names):
                val = row[i]
                if val is None:
                    rec[col] = ""
                elif hasattr(val, "strftime"):
                    rec[col] = val.strftime("%m/%d/%Y")
                else:
                    rec[col] = str(val)
            out.append(rec)

        return JsonResponse({"success": True, "data": out})

    except Exception as e:
        print(f"Error in AjaxCallForDemandsChart: {e}")
        traceback.print_exc()
        return JsonResponse({"success": False, "error": str(e)})


def AjaxCallForDemandsByAccountName(request):
    """Demands by Account Name chart endpoint — returns total demands per account
    broken down by tower, sorted by total demands descending (left-to-right on chart)."""
    try:
        idx_label = request.GET.get("idx_label")
        limit = request.GET.get("limit", "all")

        if not idx_label:
            index_labels = getIndexLabels()
            idx_label = index_labels[0] if index_labels else None
        if not idx_label:
            return JsonResponse({"success": False, "error": "No upload date available"})

        try:
            dt = datetime.strptime(idx_label, "%m/%d/%Y")
            sql_date = dt.strftime("%Y-%m-%d")
        except Exception:
            sql_date = idx_label

        top_clause = ""
        if limit and str(limit).lower() != "all":
            try:
                top_clause = f"TOP {int(limit)}"
            except ValueError:
                top_clause = ""

        f = _build_pipeline_filters(request, idx_label)

        where_parts = ["p.UploadedOn = %s"]
        params = [sql_date]

        _append_optional_filter(where_parts, params, "p.[R_Vertical]", f["verticals"], skip_if_all=False)
        _append_optional_filter(where_parts, params, "p.[Market Unit]", f["regions"], skip_if_all=False)
        _append_optional_filter(where_parts, params, "p.[Sales Stage]", f["salesstages"])
        _append_optional_filter(where_parts, params, "p.[DealSize]", f["dealsizes"], skip_if_all=False)
        _append_optional_filter(where_parts, params, "p.[R_Deal Type]", f["opptypes"], skip_if_all=False)
        _append_optional_filter(where_parts, params, "d.[SO Line Status]", f["so_statuses"], skip_if_all=False)

        _append_optional_filter(where_parts, params, "p.[BU]", f["bus"])
        _append_optional_filter(where_parts, params, "p.[SBU1]", f["sbus"])
        _append_optional_filter(where_parts, params, "p.[Account Name]", f["accounts"])

        where_sql = " AND ".join(where_parts)

        # Dedicated query — groups exclusively by Account Name, ordered descending
        # so the chart renders largest → smallest left to right.
        query = f"""
            SELECT {top_clause}
                p.[Account Name],
                COUNT(DISTINCT CASE WHEN d.Tower = N'ADM'                       THEN d.[Unique ID] END) AS [ADM],
                COUNT(DISTINCT CASE WHEN d.Tower = N'AIA'                       THEN d.[Unique ID] END) AS [AIA],
                COUNT(DISTINCT CASE WHEN d.Tower = N'Business Support'          THEN d.[Unique ID] END) AS [Business Support],
                COUNT(DISTINCT CASE WHEN d.Tower = N'CIS'                       THEN d.[Unique ID] END) AS [CIS],
                COUNT(DISTINCT CASE WHEN d.Tower = N'Cognizant Moment'          THEN d.[Unique ID] END) AS [Cognizant Moment],
                COUNT(DISTINCT CASE WHEN d.Tower = N'Consulting'                THEN d.[Unique ID] END) AS [Consulting],
                COUNT(DISTINCT CASE WHEN d.Tower = N'Corporate'                 THEN d.[Unique ID] END) AS [Corporate],
                COUNT(DISTINCT CASE WHEN d.Tower = N'Cybersecurity'             THEN d.[Unique ID] END) AS [Cybersecurity],
                COUNT(DISTINCT CASE WHEN d.Tower = N'Digital Engineering'       THEN d.[Unique ID] END) AS [Digital Engineering],
                COUNT(DISTINCT CASE WHEN d.Tower = N'EPS'                       THEN d.[Unique ID] END) AS [EPS],
                COUNT(DISTINCT CASE WHEN d.Tower = N'Industry Solutions Group'  THEN d.[Unique ID] END) AS [Industry Solutions Group],
                COUNT(DISTINCT CASE WHEN d.Tower = N'IOA'                       THEN d.[Unique ID] END) AS [IOA],
                COUNT(DISTINCT CASE WHEN d.Tower = N'IoT'                       THEN d.[Unique ID] END) AS [IoT],
                COUNT(DISTINCT CASE WHEN d.Tower = N'MDU'                       THEN d.[Unique ID] END) AS [MDU],
                COUNT(DISTINCT CASE WHEN d.Tower = N'Others'                    THEN d.[Unique ID] END) AS [Others],
                COUNT(DISTINCT CASE WHEN d.Tower = N'QEA'                       THEN d.[Unique ID] END) AS [QEA],
                COUNT(DISTINCT CASE WHEN d.Tower = N'ServiceNow Business Group' THEN d.[Unique ID] END) AS [ServiceNow Business Group],
                COUNT(DISTINCT d.[Unique ID]) AS [Total Demands]
            FROM dbo.Pipelinedata AS p
            INNER JOIN dbo.demand AS d
                ON p.[WinZone Opportunity ID] = d.[Opportunity ID]
            WHERE {where_sql}
            GROUP BY p.[Account Name]
            ORDER BY [Total Demands] DESC
        """

        with connection.cursor() as cursor:
            cursor.execute(query, params)
            col_names = [col[0] for col in cursor.description]
            rows = cursor.fetchall()

        out = []
        for row in rows:
            rec = {}
            for i, col in enumerate(col_names):
                val = row[i]
                if val is None:
                    rec[col] = 0 if col != "Account Name" else ""
                else:
                    rec[col] = int(val) if isinstance(val, (int, numpy.integer)) else str(val)
            out.append(rec)

        return JsonResponse({"success": True, "data": out})

    except Exception as e:
        print(f"Error in AjaxCallForDemandsByAccountName: {e}")
        traceback.print_exc()
        return JsonResponse({"success": False, "error": str(e)})


def AjaxCallForDemandsReqStartDate(request):
    """Demands by Requirement Start Date chart endpoint — returns total demands per unique
    requirement start date broken down by tower, sorted ascending (oldest → newest)."""
    try:
        idx_label = request.GET.get("idx_label")
        limit = request.GET.get("limit", "all")

        if not idx_label:
            index_labels = getIndexLabels()
            idx_label = index_labels[0] if index_labels else None
        if not idx_label:
            return JsonResponse({"success": False, "error": "No upload date available"})

        try:
            dt = datetime.strptime(idx_label, "%m/%d/%Y")
            sql_date = dt.strftime("%Y-%m-%d")
        except Exception:
            sql_date = idx_label

        top_clause = ""
        if limit and str(limit).lower() != "all":
            try:
                top_clause = f"TOP {int(limit)}"
            except ValueError:
                top_clause = ""

        f = _build_pipeline_filters(request, idx_label)

        where_parts = ["p.UploadedOn = %s"]
        params = [sql_date]

        _append_optional_filter(where_parts, params, "p.[R_Vertical]", f["verticals"], skip_if_all=False)
        _append_optional_filter(where_parts, params, "p.[Market Unit]", f["regions"], skip_if_all=False)
        _append_optional_filter(where_parts, params, "p.[Sales Stage]", f["salesstages"])
        _append_optional_filter(where_parts, params, "p.[DealSize]", f["dealsizes"], skip_if_all=False)
        _append_optional_filter(where_parts, params, "p.[R_Deal Type]", f["opptypes"], skip_if_all=False)
        _append_optional_filter(where_parts, params, "d.[SO Line Status]", f["so_statuses"], skip_if_all=False)

        _append_optional_filter(where_parts, params, "p.[BU]", f["bus"])
        _append_optional_filter(where_parts, params, "p.[SBU1]", f["sbus"])
        _append_optional_filter(where_parts, params, "p.[Account Name]", f["accounts"])

        where_sql = " AND ".join(where_parts)

        # Dedicated query — groups exclusively by Requirement Start Date from the demand
        # table, ordered ascending so the chart renders oldest → newest left to right.
        query = f"""
            SELECT {top_clause}
                d.[Requirement Start Date],
                COUNT(DISTINCT CASE WHEN d.Tower = N'ADM'                       THEN d.[Unique ID] END) AS [ADM],
                COUNT(DISTINCT CASE WHEN d.Tower = N'AIA'                       THEN d.[Unique ID] END) AS [AIA],
                COUNT(DISTINCT CASE WHEN d.Tower = N'Business Support'          THEN d.[Unique ID] END) AS [Business Support],
                COUNT(DISTINCT CASE WHEN d.Tower = N'CIS'                       THEN d.[Unique ID] END) AS [CIS],
                COUNT(DISTINCT CASE WHEN d.Tower = N'Cognizant Moment'          THEN d.[Unique ID] END) AS [Cognizant Moment],
                COUNT(DISTINCT CASE WHEN d.Tower = N'Consulting'                THEN d.[Unique ID] END) AS [Consulting],
                COUNT(DISTINCT CASE WHEN d.Tower = N'Corporate'                 THEN d.[Unique ID] END) AS [Corporate],
                COUNT(DISTINCT CASE WHEN d.Tower = N'Cybersecurity'             THEN d.[Unique ID] END) AS [Cybersecurity],
                COUNT(DISTINCT CASE WHEN d.Tower = N'Digital Engineering'       THEN d.[Unique ID] END) AS [Digital Engineering],
                COUNT(DISTINCT CASE WHEN d.Tower = N'EPS'                       THEN d.[Unique ID] END) AS [EPS],
                COUNT(DISTINCT CASE WHEN d.Tower = N'Industry Solutions Group'  THEN d.[Unique ID] END) AS [Industry Solutions Group],
                COUNT(DISTINCT CASE WHEN d.Tower = N'IOA'                       THEN d.[Unique ID] END) AS [IOA],
                COUNT(DISTINCT CASE WHEN d.Tower = N'IoT'                       THEN d.[Unique ID] END) AS [IoT],
                COUNT(DISTINCT CASE WHEN d.Tower = N'MDU'                       THEN d.[Unique ID] END) AS [MDU],
                COUNT(DISTINCT CASE WHEN d.Tower = N'Others'                    THEN d.[Unique ID] END) AS [Others],
                COUNT(DISTINCT CASE WHEN d.Tower = N'QEA'                       THEN d.[Unique ID] END) AS [QEA],
                COUNT(DISTINCT CASE WHEN d.Tower = N'ServiceNow Business Group' THEN d.[Unique ID] END) AS [ServiceNow Business Group],
                COUNT(DISTINCT d.[Unique ID]) AS [Total Demands]
            FROM dbo.Pipelinedata AS p
            INNER JOIN dbo.demand AS d
                ON p.[WinZone Opportunity ID] = d.[Opportunity ID]
            WHERE {where_sql}
            GROUP BY d.[Requirement Start Date]
            ORDER BY d.[Requirement Start Date] ASC
        """

        with connection.cursor() as cursor:
            cursor.execute(query, params)
            col_names = [col[0] for col in cursor.description]
            rows = cursor.fetchall()

        out = []
        for row in rows:
            rec = {}
            for i, col in enumerate(col_names):
                val = row[i]
                if val is None:
                    rec[col] = 0 if col != "Requirement Start Date" else ""
                else:
                    rec[col] = int(val) if isinstance(val, (int, numpy.integer)) else str(val)
            out.append(rec)

        return JsonResponse({"success": True, "data": out})

    except Exception as e:
        print(f"Error in AjaxCallForDemandsReqStartDate: {e}")
        traceback.print_exc()
        return JsonResponse({"success": False, "error": str(e)})


def AjaxCallForSunburstData(request):
    """Sunburst chart data endpoint — returns hierarchical data:
    EMEA → Vertical → Market Unit → Account Name → Opportunity Name → Tower
    with demand counts and TCV for each leaf."""
    try:
        idx_label = request.GET.get("idx_label")

        if not idx_label:
            index_labels = getIndexLabels()
            idx_label = index_labels[0] if index_labels else None
        if not idx_label:
            return JsonResponse({"success": False, "error": "No upload date available"})

        try:
            dt = datetime.strptime(idx_label, "%m/%d/%Y")
            sql_date = dt.strftime("%Y-%m-%d")
        except Exception:
            sql_date = idx_label

        f = _build_pipeline_filters(request, idx_label)

        where_parts = ["p.UploadedOn = %s"]
        params = [sql_date]

        _append_optional_filter(where_parts, params, "p.[R_Vertical]", f["verticals"], skip_if_all=False)
        _append_optional_filter(where_parts, params, "p.[Market Unit]", f["regions"], skip_if_all=False)
        _append_optional_filter(where_parts, params, "p.[Sales Stage]", f["salesstages"])
        _append_optional_filter(where_parts, params, "p.[DealSize]", f["dealsizes"], skip_if_all=False)
        _append_optional_filter(where_parts, params, "p.[R_Deal Type]", f["opptypes"], skip_if_all=False)
        _append_optional_filter(where_parts, params, "d.[SO Line Status]", f["so_statuses"], skip_if_all=False)

        towers = request.GET.getlist("towers[]")
        _append_optional_filter(where_parts, params, "d.[Tower]", towers, skip_if_all=False)

        _append_optional_filter(where_parts, params, "p.[BU]", f["bus"])
        _append_optional_filter(where_parts, params, "p.[SBU1]", f["sbus"])
        _append_optional_filter(where_parts, params, "p.[Account Name]", f["accounts"])

        where_sql = " AND ".join(where_parts)

        query = f"""
            SELECT
                p.[R_Vertical],
                p.[Market Unit],
                p.[Account Name],
                p.[Opportunity Name],
                d.[Tower],
                d.[Opportunity ID],
                COUNT(DISTINCT d.[Unique ID]) AS [Demands],
                SUM(p.[Gross TCV $])           AS [TCV]
            FROM dbo.Pipelinedata AS p
            INNER JOIN dbo.demand AS d
                ON p.[WinZone Opportunity ID] = d.[Opportunity ID]
            WHERE {where_sql}
            GROUP BY
                p.[R_Vertical],
                p.[Market Unit],
                p.[Account Name],
                p.[Opportunity Name],
                d.[Tower],
                d.[Opportunity ID]
            ORDER BY
                p.[R_Vertical],
                p.[Market Unit],
                p.[Account Name],
                p.[Opportunity Name],
                d.[Tower]
        """

        with connection.cursor() as cursor:
            cursor.execute(query, params)
            col_names = [col[0] for col in cursor.description]
            rows = cursor.fetchall()

        out = []
        for row in rows:
            rec = {}
            for i, col in enumerate(col_names):
                val = row[i]
                if val is None:
                    rec[col] = 0 if col in ("Demands", "TCV") else ""
                elif isinstance(val, (int, float)):
                    rec[col] = val
                else:
                    rec[col] = str(val)
            out.append(rec)

        return JsonResponse({"success": True, "data": out})

    except Exception as e:
        print(f"Error in AjaxCallForSunburstData: {e}")
        traceback.print_exc()
        return JsonResponse({"success": False, "error": str(e)})


# ═══════════════════════════════════════════════════════════════════════════════
#  DEMAND UPLOAD – Multi-step wizard (Upload → Map → Preview → Execute)
# ═══════════════════════════════════════════════════════════════════════════════
from difflib import SequenceMatcher
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_POST
from django.http import StreamingHttpResponse
import math
import pyodbc


def _progress_line(stage, pct, **extra):
    """Return a single NDJSON progress line for streaming responses."""
    payload = {"stage": stage, "pct": pct, **extra}
    return json.dumps(payload) + "\n"


def _stream_error(error_msg):
    """Return a StreamingHttpResponse with a single NDJSON error result.
    Use this instead of JsonResponse for early-exit errors in streaming endpoints."""
    def _gen():
        yield _progress_line("done", 100, result={"success": False, "error": error_msg})
    return StreamingHttpResponse(_gen(), content_type="text/plain")


import tempfile, os, uuid

# Directory for cached preprocessed DataFrames (temp parquet files)
_UPLOAD_CACHE_DIR = os.path.join(tempfile.gettempdir(), "demand_upload_cache")
os.makedirs(_UPLOAD_CACHE_DIR, exist_ok=True)


def _save_df_to_cache(df):
    """Save a DataFrame to a temp pickle file on disk. Returns the cache key."""
    key = uuid.uuid4().hex
    path = os.path.join(_UPLOAD_CACHE_DIR, f"{key}.pkl")
    df.to_pickle(path)
    return key


def _load_df_from_cache(key):
    """Load a DataFrame from a temp pickle file on disk. Returns the DataFrame."""
    path = os.path.join(_UPLOAD_CACHE_DIR, f"{key}.pkl")
    if not os.path.exists(path):
        raise FileNotFoundError(f"Cached file not found for key {key}. Please re-upload.")
    return pd.read_pickle(path)


def _remove_cache(key):
    """Remove a cached pickle file."""
    if key:
        path = os.path.join(_UPLOAD_CACHE_DIR, f"{key}.pkl")
        try:
            os.remove(path)
        except OSError:
            pass


# ── Column list for dbo.demand (order must match the INSERT statement) ──
DEMAND_COLUMNS = [
    "Unique ID","SO Line Status","Owning Organization","Pool ID","Pool Name",
    "Department","Vertical","Practice","SubVertical","SubPractice","BU","BU_New",
    "BusinessUnit Desc","SBU1","SBU2","Account ID","Account Name","Parent Customer",
    "Project ID","Project Name","Project Type","Project Billability Type",
    "Associate Previous Project","Associate Previous Account",
    "Associate Previous Department","Associate Fulfilled against the SO",
    "Associate Hired Grade","Quantity","Action Date","Action Week",
    "SO Submission Date","Offer Created Date","Offer Extended Date",
    "Available positions in RR","Offer Status","Offer Sub Status","No Of Offers",
    "Job Opening Status","Recruiter ID","Recruiter Name","Hiring Manager",
    "Subcontractor Allowed by Customer","Interview Required by Customer",
    "T&MRateCard","Assignment Start Date","Job Code","Flagged for Recruitment",
    "When Flagged for Recruitment","Cancelled BY ID","Cancellation Reason",
    "Cancellation_comments","Off/ On","Geography","Country","City",
    "Preferred Location 1","Preferred Location 2","Fulfilment/Cancellation Month",
    "Requirement Start Date","Requirement End Date","SO Billability",
    "Additional Revenue","Billability Start date","INTERNAL FULFILMENT-TAT",
    "EXTERNAL FULFILMENT- WFM -TAT","EXTERNAL FULFILMENT- TAG -TAT",
    "TAT(Flag dt to Interview dt)","TAT(Int to Offer creation)",
    "TAT(Offer create to Offer approve)","TAT(Offer Apprvd to Offer Extnd)",
    "TAT(Offer extnd -EDOJ)","TAT(Exp DOJ- DOJ)","Source category",
    "Percentile Range(Sal)","Cancellation Ageing","Open SO Ageing","RR Ageing",
    "Open SO Ageing range","RR Ageing range","Market","Market Unit","SO TYPE",
    "SO GRADE","ServiceLine","Service Line Description","Track","Track Description",
    "Sub Track","Sub Track Description","Demand Role Code","Demand Role Description",
    "Technical Skills Required","Technical Skills Desired","Functional Skills",
    "Leadership and Prof Dev Comp","Additional Skills","Skill Family","RLC","RSC1",
    "Domain Skill Layer 1","Domain Skill Layer 2","Domain Skill Layer 3",
    "Domain Skill Layer 4","Requirement type","Revenue potential",
    "Revenue Loss Category","Staffing Team Member","Staffing Team Lead","SoStatus",
    "TMP SO Status","Probable Fullfilment Date","Entered By",
    "Open Trained Associate","Primary Skill Set","Opportunity ID",
    "Expected Date Of Joining","Replaced Associate","Customer Bill Rate",
    "Bill rate currency","Customer Profitability","OE Approval Flag","OE Approver ID",
    "OE Approver Date","OE Approval Comments","TSC Approval Flag","TSC Approver ID",
    "TSC Approver Date","TSC Approval Comments","Customer Project",
    "Primary State tag","Secondary State tag","status_remark","Opportunity Status",
    "Job Description","Revenue","GreenChannel","Forecast Category","Win Probability",
    "Estimated Deal close date","Actual Expected Revenue Start date",
    "Opportunity Owner","OwnerID","Recommended for Hiring By",
    "Recommended for Hiring On","Tower","Service Line","Vertical Grouping",
    "New Market","Market Org","Off/On/Nearshore","Off/On (Market)","FTE/CWR",
    "New LT","NEW LT REQ GRP","PD LT Grouping",
    "Lead time (>30 days) \u2013 Yes or No","Billing loss",
    "Days until Req Start date","Days until Req Start date grouping",
    "Open SO/RR Ageing","Open SO/RR Ageing Group","Promotion SO","ELT/Non ELT",
    "True Demand","TD Classification","True Demand 15th Apr'25 Refresh",
    "True Demand - 7th Nov","# of Active Proposals","Active Proposals","Deflag MFR",
    "Past Due Demand","Open PD Ageing Bucket","Requirement Month",
    "Requirement Month-Week","Requirement Qtr","Req Year","Fulfilled",
    "Int Fulfilled","Ext Fulfilled","Cancelled","Total Demand","Open",
    "With TSC/ TAG","Demand Status","Demand Sub Status","PD Demand Sub Status",
    "EXANT","EXTN","Offer Status (From Latest RR Funnel Report)","Expected DOJ",
    "Expected DOJ (Month)","Expected DOJ (Weekly)","26th May PD status ","Check",
    "26th-May Req Start Dt","check 1","Flagged for Recruitment (Prism actual)",
    "> 70% Auto Allocation","> 70% Auto Allocation (Proposal Status)",
    "TMP Status (as on 9th Jul'25)"," CogniBOT True Demand Status - 5th Sep",
    "GGM/Non GGM","Manual Demand","Focus Accounts","UID",
    "Int Fulfillment Supply Pool","Original Requirement Start date",
    "Change in Requirement Start date","# of Days (Diff in Req Star dates)",
    "Diff in Req Start Dt Grouping","Original LT","Original LT Group",
    "SO Work Model","Project Classification","DS - TSC",
    "SO Grade vs Associate Grade","Dept Classification","PD Status group",
    "Actioned Status","New SL","New Practice","Past Due + Due in next 45 days",
    "# of Active Proposals (>40)","# of Active Proposals (>70","RI %",
]

# ── helpers ──

def _fuzzy_match(excel_header, db_columns, threshold=1):
    """Return the best fuzzy match (column, score) for an Excel header."""
    best, best_score = None, 0.0
    norm = excel_header.strip().lower()
    for col in db_columns:
        score = SequenceMatcher(None, norm, col.strip().lower()).ratio()
        if score > best_score:
            best, best_score = col, score
    if best_score >= threshold:
        return best, round(best_score, 2)
    return None, 0.0


def _get_demand_db_columns():
    """Fetch actual column names from dbo.demand via INFORMATION_SCHEMA."""
    with connection.cursor() as cur:
        cur.execute(
            "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
            "WHERE TABLE_SCHEMA='dbo' AND TABLE_NAME='demand' ORDER BY ORDINAL_POSITION"
        )
        return [r[0] for r in cur.fetchall()]


def _get_demand_column_meta():
    """Return dict of column_name -> {data_type, max_length, is_nullable, precision, scale}."""
    with connection.cursor() as cur:
        cur.execute("""
            SELECT COLUMN_NAME, DATA_TYPE,
                   CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE,
                   NUMERIC_PRECISION, NUMERIC_SCALE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA='dbo' AND TABLE_NAME='demand'
        """)
        meta = {}
        for name, dtype, maxlen, nullable, precision, scale in cur.fetchall():
            meta[name] = {
                "data_type": dtype,
                "max_length": maxlen,
                "is_nullable": nullable == "YES",
                "precision": precision,
                "scale": scale,
            }
        return meta


# ── Preprocessing constants ──

DATE_COLUMNS_TO_CONVERT = [
    "Action date", "SO Submission Date", "Offer Created Date",
    "Offer Extended Date", "Assignment Start Date", "Requirement Start Date",
    "Requirement End Date", "Billability Start date",
    "Fulfilment/Cancellation Month", "Probable Fullfilment Date",
    "Expected Date Of Joining", "OE Approver Date", "TSC Approver Date",
    "Estimated Deal close date", "Actual Expected Revenue Start date",
    "Deflag MFR Date", "Original Requirement Start date",
    "Assignment Staging Date",
]

NEARSHORE_COUNTRIES = {
    "china", "hungary", "latvia", "lithuania", "philippines",
    "poland", "portugal", "romania", "spain",
}

# Practice → Tower lookup (from ref.xlsx Sheet1 columns A→B)
PRACTICE_TOWER_MAP = {
    "adm application development": "ADM",
    "adm": "ADM",
    "avm": "ADM",
    "adm central": "ADM",
    "aia data": "AIA",
    "aia": "AIA",
    "aia intelligence": "AIA",
    "aia top": "AIA",
    "cis business experience svcs": "CIS",
    "cis business foundation servcs": "CIS",
    "cis central": "CIS",
    "cis cloud services": "CIS",
    "cis infra managed svcs": "CIS",
    "corporate": "Corporate",
    "deployable pool": "Others",
    "cybersecurity": "Cybersecurity",
    "de enterprise engineering": "Digital Engineering",
    "de studio": "Digital Engineering",
    "enablement business support": "Business Support",
    "enterprise automation": "Business Support",
    "eps central": "EPS",
    "eps ipm": "EPS",
    "eps oracle": "EPS",
    "eps pega": "EPS",
    "eps sap": "EPS",
    "eps supply chain management": "EPS",
    "eps workday": "EPS",
    "global consulting bfs": "Consulting",
    "global consulting cmt": "Consulting",
    "global consulting ent proc": "Consulting",
    "global consulting gov&ps": "Consulting",
    "global consulting hc": "Consulting",
    "global consulting ins": "Consulting",
    "global consulting ls": "Consulting",
    "global consulting mleu": "Consulting",
    "global consulting rcgth": "Consulting",
    "global consulting scm": "Consulting",
    "global consulting tech": "Consulting",
    "global consulting trans mgmt": "Consulting",
    "ioa business operations": "IOA",
    "ioa central": "IOA",
    "ioa ipa group": "IOA",
    "iot central": "IoT",
    "iot commercial solutions": "IoT",
    "iot industrial operations": "IoT",
    "iot mobica": "IoT",
    "iot product engineering": "IoT",
    "isg bfs group": "Industry Solutions Group",
    "isg cmt group": "Industry Solutions Group",
    "isg insurance group": "Industry Solutions Group",
    "isg ls commercial group": "Industry Solutions Group",
    "isg ls lab group": "Industry Solutions Group",
    "isg ls manufacturing group": "Industry Solutions Group",
    "isg ls r&d group": "Industry Solutions Group",
    "isg rcgth group": "Industry Solutions Group",
    "mdu": "MDU",
    "moment central": "Cognizant Moment",
    "moment cx crm": "Cognizant Moment",
    "moment digital experience": "Cognizant Moment",
    "ppm coe": "Others",
    "product engineering": "Others",
    "pure vertical": "MDU",
    "qea": "QEA",
    "rdc": "Business Support",
    "servicenow": "ServiceNow",
    "servicenow business group": "ServiceNow",
    "sp&e central": "Digital Engineering",
    "sustainability": "IoT",
    "iot sustainability": "IoT",
}


def _preprocess_demand_df(df, on_progress=None):
    """
    Apply mandatory preprocessing transformations on the uploaded DataFrame
    before any column mapping, validation, or database operations.
    Returns the modified DataFrame.
    on_progress(stage, pct) — optional callback to report progress.
    """
    import logging
    logger = logging.getLogger(__name__)
    emit = on_progress or (lambda stage, pct: None)

    # ── Subtask 1.1: Convert date columns to datetime ──
    emit("Converting date columns to Short Date format", 20)
    col_lookup = {c.strip().lower(): c for c in df.columns}
    for date_col_name in DATE_COLUMNS_TO_CONVERT:
        actual_col = col_lookup.get(date_col_name.strip().lower())
        if actual_col and actual_col in df.columns:
            df[actual_col] = pd.to_datetime(df[actual_col], errors='coerce', format='mixed', dayfirst=False)
            logger.info(f"[PREPROCESS] Converted '{actual_col}' to datetime")

    # ── Subtask 1.2: Create Off/On/Nearshore column ──
    emit("Creating Off/On/Nearshore column", 40)
    off_on_col = col_lookup.get("off/ on")
    country_col = col_lookup.get("country")
    if off_on_col and off_on_col in df.columns:
        # Start by copying Off/ On values
        df["Off/On/Nearshore"] = df[off_on_col].copy()
        if country_col and country_col in df.columns:
            # Where country is in Nearshore list, override to "Nearshore"
            country_norm = df[country_col].astype(str).str.strip().str.lower()
            nearshore_mask = country_norm.isin(NEARSHORE_COUNTRIES)
            df.loc[nearshore_mask, "Off/On/Nearshore"] = "Nearshore"
        logger.info(f"[PREPROCESS] Created 'Off/On/Nearshore' column")
    else:
        logger.warning(f"[PREPROCESS] 'Off/ On' column not found, skipping Off/On/Nearshore creation")

    # ── Subtask 1.3: Create Tower column ──
    emit("Creating Tower column from Practice lookup", 55)
    practice_col = col_lookup.get("practice")
    if practice_col and practice_col in df.columns:
        def _lookup_tower(val):
            if pd.isna(val) or str(val).strip() == "":
                return "Others"
            key = str(val).strip().lower()
            return PRACTICE_TOWER_MAP.get(key, "Others")
        df["Tower"] = df[practice_col].apply(_lookup_tower)
        logger.info(f"[PREPROCESS] Created 'Tower' column from '{practice_col}'")
    else:
        logger.warning(f"[PREPROCESS] 'Practice' column not found, skipping Tower creation")

    # ── Subtask 1.4: Rename columns (careful sequencing) ──
    emit("Renaming columns (ServiceLine, CCA mappings)", 70)
    col_lookup = {c.strip().lower(): c for c in df.columns}  # refresh after new cols

    has_serviceline = "serviceline" in col_lookup
    has_cca_service_line = "cca service line" in col_lookup
    has_cca_service_line_desc = "cca service line description" in col_lookup

    rename_map = {}

    # Step 1: If original "ServiceLine" exists, rename to temp to avoid collision
    if has_serviceline and has_cca_service_line:
        actual_sl = col_lookup["serviceline"]
        rename_map[actual_sl] = "_temp_ServiceLine"
        logger.info(f"[PREPROCESS] Renaming '{actual_sl}' → '_temp_ServiceLine' (temporary)")

    # Apply temp rename first
    if rename_map:
        df = df.rename(columns=rename_map)
        rename_map = {}

    # Step 2: Rename "CCA Service Line" → "ServiceLine"
    # Refresh lookup after temp rename
    col_lookup = {c.strip().lower(): c for c in df.columns}
    if has_cca_service_line:
        actual_cca = col_lookup.get("cca service line")
        if actual_cca:
            rename_map[actual_cca] = "ServiceLine"
            logger.info(f"[PREPROCESS] Renaming '{actual_cca}' → 'ServiceLine'")

    # Step 3: Rename temp → "Service Line"
    if "_temp_ServiceLine" in df.columns:
        rename_map["_temp_ServiceLine"] = "Service Line"
        logger.info(f"[PREPROCESS] Renaming '_temp_ServiceLine' → 'Service Line'")
    elif has_serviceline and not has_cca_service_line:
        # No collision: just rename ServiceLine → Service Line
        actual_sl = col_lookup.get("serviceline")
        if actual_sl:
            rename_map[actual_sl] = "Service Line"
            logger.info(f"[PREPROCESS] Renaming '{actual_sl}' → 'Service Line'")

    # Step 4: Rename "CCA Service Line Description" → "Service Line Description"
    if has_cca_service_line_desc:
        actual_cca_desc = col_lookup.get("cca service line description")
        if actual_cca_desc:
            rename_map[actual_cca_desc] = "Service Line Description"
            logger.info(f"[PREPROCESS] Renaming '{actual_cca_desc}' → 'Service Line Description'")

    if rename_map:
        df = df.rename(columns=rename_map)

    # ── Subtask 1.5: Fix column names with dots ──
    dot_renames = {}
    for c in df.columns:
        if "Leadership and Prof. Dev. Comp" in c:
            dot_renames[c] = "Leadership and Prof Dev Comp"
    if dot_renames:
        df = df.rename(columns=dot_renames)
        for old, new in dot_renames.items():
            logger.info(f"[PREPROCESS] Renaming '{old}' → '{new}'")

    logger.info(f"[PREPROCESS] Preprocessing complete. DataFrame shape: {df.shape}")
    return df


# ── Step 0: render the wizard page ──

def demand_upload_page(request):
    """Render the multi-step demand upload wizard."""
    return render(request, "visualize/demand_upload.html", {
        "plot_label": "Demand Upload",
    })


# ── Step 1 AJAX: upload file → return header mapping suggestions ──

@csrf_exempt
@require_POST
def demand_upload_file(request):
    """
    Accepts the .xlsx file (+ optional UploadedOn date) in a multipart POST.
    Reads headers, auto-maps them to dbo.demand columns, returns JSON mapping.
    Streams NDJSON progress lines so the frontend can show real-time progress.
    """
    # --- Validate inputs synchronously (before streaming) ---
    f = request.FILES.get("file")
    if not f:
        return _stream_error("No file uploaded.")
    if not f.name.lower().endswith(".xlsx"):
        return _stream_error("Only .xlsx files are accepted.")
    if f.size > 100 * 1024 * 1024:
        return _stream_error("File exceeds 100 MB limit.")

    uploaded_on = request.POST.get("uploaded_on", "")
    raw = f.read()  # read file bytes before entering generator

    # ── Do ALL session-writing work synchronously (before the generator) ──
    # StreamingHttpResponse generators run AFTER Django's session middleware,
    # so session writes inside generators are silently lost.
    import io
    try:
        df_full = pd.read_excel(io.BytesIO(raw), engine="openpyxl")
        total_rows = int(df_full.shape[0])
        df_full = _preprocess_demand_df(df_full)
        cache_key = _save_df_to_cache(df_full)
        request.session["_demand_cache_key"] = cache_key
        request.session["_demand_uploaded_on"] = uploaded_on
        request.session.save()
    except Exception as e:
        traceback.print_exc()
        return _stream_error(str(e))

    # ── Stream only the lightweight mapping phase ──
    def _stream():
        try:
            yield _progress_line("Reading and preprocessing complete", 75)

            yield _progress_line("Matching columns to database schema", 88)
            excel_headers = df_full.columns.tolist()
            db_cols = _get_demand_db_columns()
            db_set = {c.strip().lower(): c for c in db_cols}

            mapping = []
            for hdr in excel_headers:
                norm = hdr.strip().lower()
                if norm in db_set:
                    mapping.append({"excel": hdr, "suggested_db": db_set[norm], "score": 1.0, "status": "exact"})
                else:
                    mapping.append({"excel": hdr, "suggested_db": "", "score": 0, "status": "unmapped"})

            yield _progress_line("done", 100, result={
                "success": True,
                "excel_headers": excel_headers,
                "db_columns": db_cols,
                "mapping": mapping,
                "row_count": len(excel_headers),
                "total_rows": total_rows,
            })
        except Exception as e:
            traceback.print_exc()
            yield _progress_line("done", 100, result={"success": False, "error": str(e)})

    return StreamingHttpResponse(_stream(), content_type="text/plain")


# ── Step 2 AJAX: preview + validate mapped data ──

@csrf_exempt
@require_POST
def demand_upload_preview(request):
    """
    Accepts the confirmed mapping as JSON, applies it to the file stored in
    session, validates the first N rows, returns preview + errors.
    Streams NDJSON progress lines for real-time frontend updates.
    """
    body = json.loads(request.body)
    col_mapping = body.get("mapping", {})
    preview_rows = int(body.get("preview_rows", 20))

    cache_key = request.session.get("_demand_cache_key")
    if not cache_key:
        return _stream_error("No file in session. Please re-upload.")

    def _stream():
        try:
            yield _progress_line("Loading cached data from disk", 10)
            df = _load_df_from_cache(cache_key)

            yield _progress_line("Applying column mapping", 30)
            rename_map = {}
            for excel_hdr, db_col in col_mapping.items():
                if db_col and excel_hdr in df.columns:
                    rename_map[excel_hdr] = db_col
            df = df.rename(columns=rename_map)
            db_cols = _get_demand_db_columns()
            keep = [c for c in df.columns if c in db_cols]
            df = df[keep]

            yield _progress_line("Validating fields and data types", 55)
            meta = _get_demand_column_meta()
            errors = []
            MAX_ERRORS = 30
            for idx, row in df.head(preview_rows).iterrows():
                for col in keep:
                    val = row[col]
                    m = meta.get(col, {})
                    if not m.get("is_nullable", True) and (pd.isna(val) or str(val).strip() == ""):
                        errors.append({"row": int(idx) + 2, "col": col, "msg": "Required field is empty"})
                    if m.get("max_length") and not pd.isna(val) and len(str(val)) > m["max_length"]:
                        errors.append({"row": int(idx) + 2, "col": col,
                                       "msg": f"Exceeds max length {m['max_length']} (got {len(str(val))})"})
                    if len(errors) >= MAX_ERRORS:
                        break
                if len(errors) >= MAX_ERRORS:
                    break

            yield _progress_line("Building preview table", 80)
            preview_df = df.head(preview_rows).copy()
            preview_df = preview_df.where(pd.notnull(preview_df), None)
            preview_data = []
            for _, r in preview_df.iterrows():
                preview_data.append({c: (str(r[c]) if r[c] is not None else "") for c in keep})

            yield _progress_line("done", 100, result={
                "success": True,
                "columns": keep,
                "preview": preview_data,
                "errors": errors[:MAX_ERRORS],
                "total_rows": int(df.shape[0]),
                "mapped_columns": len(keep),
            })

        except Exception as e:
            traceback.print_exc()
            yield _progress_line("done", 100, result={"success": False, "error": str(e)})

    return StreamingHttpResponse(_stream(), content_type="text/plain")


# ── Step 3 AJAX: confirm & execute (transactional) ──

@csrf_exempt
@require_POST
def demand_upload_execute(request):
    """
    Final step: reads the full file from session, applies mapping, then in a
    single transaction:
      1. DELETE FROM dbo.demand
      2. Bulk-INSERT into dbo.demand
      3. Remove CANCELLED rows
      4. Remove non-EMEA rows
      5. INSERT INTO dbo.demand_history SELECT … FROM dbo.demand
      6. COMMIT (or ROLLBACK on error)
    Streams NDJSON progress lines for real-time frontend updates.
    """
    body = json.loads(request.body)
    col_mapping = body.get("mapping", {})
    uploaded_on = body.get("uploaded_on", "")

    if not uploaded_on:
        return _stream_error("UploadedOn date is required.")

    cache_key = request.session.get("_demand_cache_key")
    if not cache_key:
        return _stream_error("No file in session. Please re-upload.")

    def _stream():
        import time, sys
        t0 = time.time()
        try:
            yield _progress_line("Loading cached data from disk", 5)
            df = _load_df_from_cache(cache_key)
            print(f"[DEMAND UPLOAD] +{time.time()-t0:.1f}s  Loaded from cache: {df.shape[0]} rows x {df.shape[1]} cols"); sys.stdout.flush()

            yield _progress_line("Applying column mapping", 10)
            rename_map = {}
            for excel_hdr, db_col in col_mapping.items():
                if db_col and excel_hdr in df.columns:
                    rename_map[excel_hdr] = db_col
            df = df.rename(columns=rename_map)

            db_cols = _get_demand_db_columns()
            keep = [c for c in df.columns if c in db_cols]
            df = df[keep]

            # Get column type metadata to know which columns are DATE
            meta = _get_demand_column_meta()
            date_cols = set(c for c in keep if meta.get(c, {}).get("data_type", "").lower() in ("date", "datetime", "datetime2", "smalldatetime"))
            float_cols = set(c for c in keep if meta.get(c, {}).get("data_type", "").lower() in ("float", "real", "decimal", "numeric", "money"))
            int_cols = set(c for c in keep if meta.get(c, {}).get("data_type", "").lower() in ("int", "bigint", "smallint", "tinyint"))

            # ── Vectorized column-level conversion (much faster than per-cell) ──
            yield _progress_line("Processing date columns", 15)
            for col in date_cols:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors='coerce', format='mixed', dayfirst=False)

            yield _progress_line(f"Converting {df.shape[0]} rows to database types", 20)

            # SQL Server integer type ranges
            _INT_RANGES = {
                "tinyint": (0, 255),
                "smallint": (-32768, 32767),
                "int": (-2147483648, 2147483647),
                "bigint": (-9223372036854775808, 9223372036854775807),
            }

            # Convert each column in bulk using pandas vectorized ops
            for col in keep:
                if col in date_cols:
                    # Convert Timestamps to Python date objects; NaT → None
                    s = df[col]
                    df[col] = s.apply(lambda v: v.date() if pd.notna(v) and isinstance(v, pd.Timestamp) else None)
                elif col in float_cols:
                    col_meta = meta.get(col, {})
                    precision = col_meta.get("precision")
                    scale = col_meta.get("scale")
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                    if precision and col_meta.get("data_type", "").lower() in ("decimal", "numeric"):
                        max_abs = 10 ** (precision - (scale or 0)) - 1
                        def _safe_decimal(v, _max=max_abs, _scale=scale or 0):
                            if not pd.notna(v):
                                return None
                            fv = float(v)
                            if math.isinf(fv):
                                return None
                            if abs(fv) > _max:
                                return None
                            return round(fv, _scale)
                        df[col] = df[col].apply(_safe_decimal)
                    else:
                        df[col] = df[col].apply(lambda v: float(v) if pd.notna(v) and not math.isinf(float(v)) else None)
                elif col in int_cols:
                    col_dtype = meta.get(col, {}).get("data_type", "int").lower()
                    lo, hi = _INT_RANGES.get(col_dtype, _INT_RANGES["bigint"])
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                    def _safe_int(v, _lo=lo, _hi=hi):
                        if not pd.notna(v):
                            return None
                        iv = int(v)
                        if iv < _lo or iv > _hi:
                            return None
                        return iv
                    df[col] = df[col].apply(_safe_int)
                else:
                    # String column: convert to str, strip, truncate, None for blanks
                    max_len = meta.get(col, {}).get("max_length")
                    def _str_convert(v, _ml=max_len):
                        if v is None:
                            return None
                        if isinstance(v, float):
                            if math.isnan(v) or math.isinf(v):
                                return None
                            return str(int(v)) if v == int(v) else str(v)
                        if isinstance(v, int):
                            return str(v)
                        if isinstance(v, pd.Timestamp):
                            return v.strftime('%Y-%m-%d') if pd.notna(v) else None
                        if isinstance(v, datetime):
                            return v.strftime('%Y-%m-%d')
                        s = str(v).strip()
                        if not s or s.lower() in ('nan', 'none', 'nat'):
                            return None
                        if _ml is not None and isinstance(_ml, int) and _ml > 0 and len(s) > _ml:
                            s = s[:_ml]
                        return s
                    df[col] = df[col].apply(_str_convert)

            # Convert DataFrame to list of tuples, sanitizing NaN/NaT → None
            # df.values can re-introduce numpy.nan for float-dtype columns,
            # so we must clean each value explicitly.
            def _sanitize_row(row):
                out = []
                for v in row:
                    if v is None:
                        out.append(None)
                    elif isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
                        out.append(None)
                    elif v is pd.NaT:
                        out.append(None)
                    elif hasattr(v, 'item'):
                        # numpy scalar → Python native
                        pv = v.item()
                        out.append(None if (isinstance(pv, float) and (math.isnan(pv) or math.isinf(pv))) else pv)
                    else:
                        out.append(v)
                return tuple(out)

            rows_data = [_sanitize_row(row) for row in df.values]

            print(f"[DEMAND UPLOAD] +{time.time()-t0:.1f}s  Row conversion complete ({len(rows_data)} rows ready)"); sys.stdout.flush()

            yield _progress_line("Building SQL statements", 30)
            col_names_sql = ", ".join(f"[{c}]" for c in keep)
            placeholders = ", ".join(["?"] * len(keep))
            insert_sql = f"INSERT INTO dbo.demand ({col_names_sql}) VALUES ({placeholders})"

            try:
                uploaded_on_date = pd.to_datetime(uploaded_on).date()
            except Exception:
                uploaded_on_date = uploaded_on

            history_meta = {}
            with connection.cursor() as _mc:
                _mc.execute("""
                    SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA='dbo' AND TABLE_NAME='demand_history'
                """)
                for _cn, _dt, _ml in _mc.fetchall():
                    history_meta[_cn] = {"data_type": _dt, "max_length": _ml}

            _insert_cols = []
            _select_exprs = []
            for c in DEMAND_COLUMNS:
                _insert_cols.append(f"[{c}]")
                h = history_meta.get(c)
                if (h and h["data_type"] in ("varchar", "nvarchar", "char", "nchar")
                        and h["max_length"] is not None and h["max_length"] > 0):
                    _select_exprs.append(f"LEFT([{c}], {h['max_length']})")
                else:
                    _select_exprs.append(f"[{c}]")

            history_insert_cols = ", ".join(_insert_cols)
            history_select_exprs = ", ".join(_select_exprs)
            history_sql = (
                f"INSERT INTO dbo.demand_history ([UploadedOn], {history_insert_cols}) "
                f"SELECT ? AS UploadedOn, {history_select_exprs} FROM dbo.demand"
            )

            # Execute inside a transaction
            raw_conn = connection.connection
            if raw_conn is None:
                connection.ensure_connection()
                raw_conn = connection.connection
            old_autocommit = raw_conn.autocommit
            raw_conn.autocommit = False
            cursor = raw_conn.cursor()

            try:
                cursor.execute("SET XACT_ABORT ON")

                # 1. DELETE existing demand data
                yield _progress_line("Deleting existing demand data", 35)
                cursor.execute("DELETE FROM dbo.demand")
                deleted = cursor.rowcount
                print(f"[DEMAND UPLOAD] +{time.time()-t0:.1f}s  Deleted {deleted} rows from dbo.demand"); sys.stdout.flush()

                # 2. Bulk INSERT via fast_executemany (NOCOUNT ON for speed)
                cursor.execute("SET NOCOUNT ON")
                cursor.fast_executemany = True
                BATCH = 20000
                inserted = 0
                total_batches = (len(rows_data) + BATCH - 1) // BATCH
                for start in range(0, len(rows_data), BATCH):
                    batch = rows_data[start:start + BATCH]
                    cursor.executemany(insert_sql, batch)
                    inserted += len(batch)
                    batch_num = start // BATCH + 1
                    batch_pct = 38 + int(32 * inserted / len(rows_data))
                    yield _progress_line(
                        f"Inserting rows: batch {batch_num}/{total_batches} ({inserted}/{len(rows_data)} rows)",
                        batch_pct
                    )
                    print(f"[DEMAND UPLOAD] +{time.time()-t0:.1f}s  Batch {batch_num}/{total_batches} ({inserted}/{len(rows_data)})"); sys.stdout.flush()
                cursor.execute("SET NOCOUNT OFF")

                # 3. Remove CANCELLED rows
                yield _progress_line("Removing CANCELLED rows from demand", 75)
                cursor.execute("DELETE FROM dbo.demand WHERE LTRIM(RTRIM([SO Line Status])) = 'CANCELLED'")
                cancelled_deleted = cursor.rowcount
                print(f"[DEMAND UPLOAD] +{time.time()-t0:.1f}s  Removed {cancelled_deleted} CANCELLED rows"); sys.stdout.flush()

                # 4. Keep only EMEA
                yield _progress_line("Filtering to EMEA market only", 80)
                cursor.execute("DELETE FROM dbo.demand WHERE LTRIM(RTRIM([Market])) != 'EMEA' OR [Market] IS NULL")
                non_emea_deleted = cursor.rowcount
                remaining = inserted - cancelled_deleted - non_emea_deleted
                print(f"[DEMAND UPLOAD] +{time.time()-t0:.1f}s  Removed {non_emea_deleted} non-EMEA rows, {remaining} remain"); sys.stdout.flush()

                # 5. Fix account name before history
                yield _progress_line("Fixing account name data", 82)
                cursor.execute("UPDATE dbo.demand SET [Account Name] = 'SIA Tele2' WHERE [Account Name] = 'SIA \"Tele2\"'")

                # 6. Copy to history
                yield _progress_line("Copying demand data to history table", 85)
                cursor.execute(history_sql, [uploaded_on_date])
                history_count = cursor.rowcount
                print(f"[DEMAND UPLOAD] +{time.time()-t0:.1f}s  Copied {history_count} rows to demand_history"); sys.stdout.flush()

                # 7. Commit
                yield _progress_line("Committing transaction", 95)
                raw_conn.commit()
                print(f"[DEMAND UPLOAD] +{time.time()-t0:.1f}s  COMMITTED. Total time: {time.time()-t0:.1f}s"); sys.stdout.flush()

                # Cleanup temp file (session cleanup is best-effort inside generator)
                _remove_cache(cache_key)
                try:
                    del request.session["_demand_cache_key"]
                    del request.session["_demand_uploaded_on"]
                    request.session.save()
                except Exception:
                    pass  # session cleanup is non-critical

                yield _progress_line("done", 100, result={
                    "success": True,
                    "deleted": deleted,
                    "inserted": inserted,
                    "cancelled_removed": cancelled_deleted,
                    "non_emea_removed": non_emea_deleted,
                    "remaining_in_demand": remaining,
                    "history_inserted": history_count,
                    "message": (
                        f"Upload complete. {inserted} rows inserted, "
                        f"{cancelled_deleted} CANCELLED rows removed, "
                        f"{non_emea_deleted} non-EMEA rows removed, "
                        f"{remaining} rows remain in demand, "
                        f"{history_count} rows copied to demand_history."
                    ),
                })

            except Exception as e:
                raw_conn.rollback()
                traceback.print_exc()
                yield _progress_line("done", 100, result={"success": False, "error": f"Transaction rolled back: {str(e)}"})
            finally:
                raw_conn.autocommit = old_autocommit

        except Exception as e:
            traceback.print_exc()
            yield _progress_line("done", 100, result={"success": False, "error": str(e)})

    return StreamingHttpResponse(_stream(), content_type="text/plain")


# ════════════════════════════════════════════════════════════════════════════
# Upload Selector Page
# ════════════════════════════════════════════════════════════════════════════

def upload_selector_page(request):
    """Render the upload type selector (Demand vs Proposal)."""
    return render(request, "visualize/upload_selector.html", {
        "plot_label": "Data Upload",
    })


# ════════════════════════════════════════════════════════════════════════════
# Proposal Upload
# ════════════════════════════════════════════════════════════════════════════

PROPOSAL_COLUMNS = [
    "Release ID", "SO ID", "SO UniqueID", "PE SO Status", "SO Grade",
    "SO Country", "SO State", "SO City", "TMP SO Pool Practice Name",
    "SO Vertical", "SO Department ID", "SO Department Name",
    "SO Pool Department ID", "SO Pool Department Name", "SO Dept",
    "SO Start Date", "SO Creation Date", "SO Status", "SO Cancelled Date",
    "SO Closed Date", "Account ID", "Account", "SO Parent Customer Id",
    "Busines Unit Code", "Busines Unit Name", "Releasing Project Vertical",
    "Hiring Manager Id", "Hiring Manager Name", "Associate Id",
    "Associate Name", "Associate IRise Status", "Associate Current Grade",
    "Job Code", "Current Location", "Proposal Type", "Proposed By Id",
    "Proposed By Name", "Proposed On", "Richness Index during proposal",
    "Blue Ring Status during proposal", "Skill Match Remarks", "Comments",
    "Rejection Feedback", "Feedback GivenBy Id",
    "Rejection Feedback given by(TSC/HM)", "Feedback Date", "Average Rating",
    "Feedback Date2", "Interview Id", "Proposed From", "Proposal Status",
    "IJM Allocation", "Country", "IsCrossBuProposal", "SOacceptedbyPOCId",
    "SOacceptedbyPOCName", "Latest Richness Index %",
    "Allocation initiated by", "Allocation auto approved",
    "Associate releasing practice", "Associate Consent Given Date",
    "Associate Consent Status", "Associate Released country",
    "Is Cross Country Proposal", "Algorator Score during proposal",
    "Latest Algorator Score", "SO Priority", "MU Priority", "Action Date",
    "Interview required by Customer (Y/N)", "Ageing", "Ageing Range",
    "Ageing Range Bucket", "SO Billability", "Billability Start Date",
    "Billability Month", "Project Type", "Project Billability Type",
    "Region (SO)", "RI %", "Flagged for Recruitment", "Market",
    "BU (Prism)", "BusinessUnit Desc (Prism)", "SBU (Prism)",
    "SO Department (Prism)", "Parent Customer", "Practice", "Service Line",
    "Vertical Grouping", "New Market", "Market Org", "Off/On (Market)",
    "TD Classification", "Past Due Demand", "Req Month", "BU_New",
    "Focus Accounts", "True Demand", "Requirement Qtr", "Off/On/Nearshore",
    "Open SO/RR Ageing Group", "SO Work Model", "Project Classification",
    "Dept Classification", "PD Status group", "Actioned Status",
]

PROPOSAL_DATE_COLUMNS = [
    "SO Start Date", "SO Cancelled Date", "SO Closed Date",
    "Feedback Date", "Feedback Date2", "Associate Consent Given Date",
    "Action Date", "SONoteUpdatedDate", "Billability Start Date",
]


def _get_proposal_db_columns():
    """Fetch actual column names from dbo.proposal_base via INFORMATION_SCHEMA."""
    with connection.cursor() as cur:
        cur.execute(
            "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
            "WHERE TABLE_SCHEMA='dbo' AND TABLE_NAME='proposal_base' ORDER BY ORDINAL_POSITION"
        )
        return [r[0] for r in cur.fetchall()]


def _get_proposal_column_meta():
    """Return dict of column_name -> {data_type, max_length, is_nullable, precision, scale} for proposal_base."""
    with connection.cursor() as cur:
        cur.execute("""
            SELECT COLUMN_NAME, DATA_TYPE,
                   CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE,
                   NUMERIC_PRECISION, NUMERIC_SCALE
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA='dbo' AND TABLE_NAME='proposal_base'
        """)
        meta = {}
        for name, dtype, maxlen, nullable, precision, scale in cur.fetchall():
            meta[name] = {
                "data_type": dtype,
                "max_length": maxlen,
                "is_nullable": nullable == "YES",
                "precision": precision,
                "scale": scale,
            }
        return meta


def _preprocess_proposal_df(df, on_progress=None):
    """Preprocess proposal DataFrame: convert date columns, fix column names."""
    import logging
    logger = logging.getLogger(__name__)
    emit = on_progress or (lambda stage, pct: None)
    emit("Converting date columns to Short Date format", 30)
    col_lookup = {c.strip().lower(): c for c in df.columns}
    for date_col_name in PROPOSAL_DATE_COLUMNS:
        actual_col = col_lookup.get(date_col_name.strip().lower())
        if actual_col and actual_col in df.columns:
            df[actual_col] = pd.to_datetime(df[actual_col], errors='coerce')

    # Fix column names with dots
    emit("Fixing column names", 60)
    dot_renames = {}
    for c in df.columns:
        if c.strip() == "Req. Month":
            dot_renames[c] = "Req Month"
    if dot_renames:
        df = df.rename(columns=dot_renames)
        for old, new in dot_renames.items():
            logger.info(f"[PREPROCESS-PROPOSAL] Renaming '{old}' → '{new}'")

    return df


# ── Proposal: render wizard page ──

def proposal_upload_page(request):
    """Render the proposal upload wizard."""
    return render(request, "visualize/proposal_upload.html", {
        "plot_label": "Proposal Upload",
    })


# ── Proposal Step 1: upload file + header mapping ──

@csrf_exempt
@require_POST
def proposal_upload_file(request):
    """Upload .xlsx, preprocess dates, return header mapping. Streams NDJSON progress."""
    f = request.FILES.get("file")
    if not f:
        return _stream_error("No file uploaded.")
    if not f.name.lower().endswith(".xlsx"):
        return _stream_error("Only .xlsx files are accepted.")
    if f.size > 100 * 1024 * 1024:
        return _stream_error("File exceeds 100 MB limit.")

    uploaded_on = request.POST.get("uploaded_on", "")
    raw = f.read()

    # ── Do ALL session-writing work synchronously ──
    import io
    try:
        df_full = pd.read_excel(io.BytesIO(raw), engine="openpyxl")
        total_rows = int(df_full.shape[0])
        df_full = _preprocess_proposal_df(df_full)
        cache_key = _save_df_to_cache(df_full)
        request.session["_proposal_cache_key"] = cache_key
        request.session["_proposal_uploaded_on"] = uploaded_on
        request.session.save()
    except Exception as e:
        traceback.print_exc()
        return _stream_error(str(e))

    # ── Stream only the lightweight mapping phase ──
    def _stream():
        try:
            yield _progress_line("Reading and preprocessing complete", 75)

            yield _progress_line("Matching columns to database schema", 88)
            excel_headers = df_full.columns.tolist()
            db_cols = _get_proposal_db_columns()
            db_set = {c.strip().lower(): c for c in db_cols}

            mapping = []
            for hdr in excel_headers:
                norm = hdr.strip().lower()
                if norm in db_set:
                    mapping.append({"excel": hdr, "suggested_db": db_set[norm], "score": 1.0, "status": "exact"})
                else:
                    mapping.append({"excel": hdr, "suggested_db": "", "score": 0, "status": "unmapped"})

            yield _progress_line("done", 100, result={
                "success": True,
                "excel_headers": excel_headers,
                "db_columns": db_cols,
                "mapping": mapping,
                "row_count": len(excel_headers),
                "total_rows": total_rows,
            })
        except Exception as e:
            traceback.print_exc()
            yield _progress_line("done", 100, result={"success": False, "error": str(e)})

    return StreamingHttpResponse(_stream(), content_type="text/plain")


# ── Proposal Step 2: preview + validate ──

@csrf_exempt
@require_POST
def proposal_upload_preview(request):
    """Apply mapping, validate, return preview. Streams NDJSON progress."""
    body = json.loads(request.body)
    col_mapping = body.get("mapping", {})
    preview_rows = int(body.get("preview_rows", 20))

    cache_key = request.session.get("_proposal_cache_key")
    if not cache_key:
        return _stream_error("No file in session. Please re-upload.")

    def _stream():
        try:
            yield _progress_line("Loading cached data from disk", 10)
            df = _load_df_from_cache(cache_key)

            yield _progress_line("Applying column mapping", 30)
            rename_map = {}
            for excel_hdr, db_col in col_mapping.items():
                if db_col and excel_hdr in df.columns:
                    rename_map[excel_hdr] = db_col
            df = df.rename(columns=rename_map)
            db_cols = _get_proposal_db_columns()
            keep = [c for c in df.columns if c in db_cols]
            df = df[keep]

            yield _progress_line("Validating fields and data types", 55)
            meta = _get_proposal_column_meta()
            errors = []
            MAX_ERRORS = 30
            for idx, row in df.head(preview_rows).iterrows():
                for col in keep:
                    val = row[col]
                    m = meta.get(col, {})
                    if not m.get("is_nullable", True) and (pd.isna(val) or str(val).strip() == ""):
                        errors.append({"row": int(idx) + 2, "col": col, "msg": "Required field is empty"})
                    if m.get("max_length") and not pd.isna(val) and len(str(val)) > m["max_length"]:
                        errors.append({"row": int(idx) + 2, "col": col,
                                       "msg": f"Exceeds max length {m['max_length']} (got {len(str(val))})"})
                    if len(errors) >= MAX_ERRORS:
                        break
                if len(errors) >= MAX_ERRORS:
                    break

            yield _progress_line("Building preview table", 80)
            preview_df = df.head(preview_rows).copy()
            preview_df = preview_df.where(pd.notnull(preview_df), None)
            preview_data = []
            for _, r in preview_df.iterrows():
                preview_data.append({c: (str(r[c]) if r[c] is not None else "") for c in keep})

            yield _progress_line("done", 100, result={
                "success": True,
                "columns": keep,
                "preview": preview_data,
                "errors": errors[:MAX_ERRORS],
                "total_rows": int(df.shape[0]),
                "mapped_columns": len(keep),
            })
        except Exception as e:
            traceback.print_exc()
            yield _progress_line("done", 100, result={"success": False, "error": str(e)})

    return StreamingHttpResponse(_stream(), content_type="text/plain")


# ── Proposal Step 3: execute (transactional) ──

@csrf_exempt
@require_POST
def proposal_upload_execute(request):
    """
    Final step for proposal upload:
      1. DELETE FROM dbo.proposal_base
      2. Bulk-INSERT into dbo.proposal_base
      3. INSERT INTO dbo.proposal_base_history SELECT … FROM dbo.proposal_base
      4. COMMIT
    Streams NDJSON progress.
    """
    body = json.loads(request.body)
    col_mapping = body.get("mapping", {})
    uploaded_on = body.get("uploaded_on", "")

    if not uploaded_on:
        return _stream_error("UploadedOn date is required.")

    cache_key = request.session.get("_proposal_cache_key")
    if not cache_key:
        return _stream_error("No file in session. Please re-upload.")

    def _stream():
        import time, sys
        t0 = time.time()
        try:
            yield _progress_line("Loading cached data from disk", 5)
            df = _load_df_from_cache(cache_key)
            print(f"[PROPOSAL UPLOAD] +{time.time()-t0:.1f}s  Loaded from cache: {df.shape[0]} rows x {df.shape[1]} cols"); sys.stdout.flush()

            yield _progress_line("Applying column mapping", 10)
            rename_map = {}
            for excel_hdr, db_col in col_mapping.items():
                if db_col and excel_hdr in df.columns:
                    rename_map[excel_hdr] = db_col
            df = df.rename(columns=rename_map)

            db_cols = _get_proposal_db_columns()
            keep = [c for c in df.columns if c in db_cols]
            df = df[keep]

            meta = _get_proposal_column_meta()
            date_cols = set(c for c in keep if meta.get(c, {}).get("data_type", "").lower() in ("date", "datetime", "datetime2", "smalldatetime"))
            float_cols = set(c for c in keep if meta.get(c, {}).get("data_type", "").lower() in ("float", "real", "decimal", "numeric", "money"))
            int_cols = set(c for c in keep if meta.get(c, {}).get("data_type", "").lower() in ("int", "bigint", "smallint", "tinyint"))

            # ── Vectorized column-level conversion (much faster than per-cell) ──
            yield _progress_line("Processing date columns", 15)
            for col in date_cols:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors='coerce', format='mixed', dayfirst=False)

            yield _progress_line(f"Converting {df.shape[0]} rows to database types", 20)

            # SQL Server integer type ranges
            _INT_RANGES = {
                "tinyint": (0, 255),
                "smallint": (-32768, 32767),
                "int": (-2147483648, 2147483647),
                "bigint": (-9223372036854775808, 9223372036854775807),
            }

            for col in keep:
                if col in date_cols:
                    s = df[col]
                    df[col] = s.apply(lambda v: v.date() if pd.notna(v) and isinstance(v, pd.Timestamp) else None)
                elif col in float_cols:
                    col_meta = meta.get(col, {})
                    precision = col_meta.get("precision")
                    scale = col_meta.get("scale")
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                    if precision and col_meta.get("data_type", "").lower() in ("decimal", "numeric"):
                        max_abs = 10 ** (precision - (scale or 0)) - 1
                        def _safe_decimal(v, _max=max_abs, _scale=scale or 0):
                            if not pd.notna(v):
                                return None
                            fv = float(v)
                            if math.isinf(fv):
                                return None
                            if abs(fv) > _max:
                                return None
                            return round(fv, _scale)
                        df[col] = df[col].apply(_safe_decimal)
                    else:
                        df[col] = df[col].apply(lambda v: float(v) if pd.notna(v) and not math.isinf(float(v)) else None)
                elif col in int_cols:
                    col_dtype = meta.get(col, {}).get("data_type", "int").lower()
                    lo, hi = _INT_RANGES.get(col_dtype, _INT_RANGES["bigint"])
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                    def _safe_int(v, _lo=lo, _hi=hi):
                        if not pd.notna(v):
                            return None
                        iv = int(v)
                        if iv < _lo or iv > _hi:
                            return None
                        return iv
                    df[col] = df[col].apply(_safe_int)
                else:
                    max_len = meta.get(col, {}).get("max_length")
                    def _str_convert(v, _ml=max_len):
                        if v is None:
                            return None
                        if isinstance(v, float):
                            if math.isnan(v) or math.isinf(v):
                                return None
                            return str(int(v)) if v == int(v) else str(v)
                        if isinstance(v, int):
                            return str(v)
                        if isinstance(v, pd.Timestamp):
                            return v.strftime('%Y-%m-%d') if pd.notna(v) else None
                        if isinstance(v, datetime):
                            return v.strftime('%Y-%m-%d')
                        s = str(v).strip()
                        if not s or s.lower() in ('nan', 'none', 'nat'):
                            return None
                        if _ml is not None and isinstance(_ml, int) and _ml > 0 and len(s) > _ml:
                            s = s[:_ml]
                        return s
                    df[col] = df[col].apply(_str_convert)

            # Convert DataFrame to list of tuples, sanitizing NaN/NaT → None
            def _sanitize_row(row):
                out = []
                for v in row:
                    if v is None:
                        out.append(None)
                    elif isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
                        out.append(None)
                    elif v is pd.NaT:
                        out.append(None)
                    elif hasattr(v, 'item'):
                        pv = v.item()
                        out.append(None if (isinstance(pv, float) and (math.isnan(pv) or math.isinf(pv))) else pv)
                    else:
                        out.append(v)
                return tuple(out)

            rows_data = [_sanitize_row(row) for row in df.values]

            yield _progress_line("Building SQL statements", 30)
            col_names_sql = ", ".join(f"[{c}]" for c in keep)
            placeholders = ", ".join(["?"] * len(keep))
            insert_sql = f"INSERT INTO dbo.proposal_base ({col_names_sql}) VALUES ({placeholders})"

            try:
                uploaded_on_date = pd.to_datetime(uploaded_on).date()
            except Exception:
                uploaded_on_date = uploaded_on

            # Build history INSERT … SELECT
            history_meta = {}
            with connection.cursor() as _mc:
                _mc.execute("""
                    SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA='dbo' AND TABLE_NAME='proposal_base_history'
                """)
                for _cn, _dt, _ml in _mc.fetchall():
                    history_meta[_cn] = {"data_type": _dt, "max_length": _ml}

            _insert_cols = []
            _select_exprs = []
            for c in PROPOSAL_COLUMNS:
                _insert_cols.append(f"[{c}]")
                h = history_meta.get(c)
                if (h and h["data_type"] in ("varchar", "nvarchar", "char", "nchar")
                        and h["max_length"] is not None and h["max_length"] > 0):
                    _select_exprs.append(f"LEFT([{c}], {h['max_length']})")
                else:
                    _select_exprs.append(f"[{c}]")

            history_insert_cols = ", ".join(_insert_cols)
            history_select_exprs = ", ".join(_select_exprs)
            history_sql = (
                f"INSERT INTO dbo.proposal_base_history ([UploadedOn], {history_insert_cols}) "
                f"SELECT ? AS UploadedOn, {history_select_exprs} FROM dbo.proposal_base"
            )

            # Execute inside a transaction
            raw_conn = connection.connection
            if raw_conn is None:
                connection.ensure_connection()
                raw_conn = connection.connection
            old_autocommit = raw_conn.autocommit
            raw_conn.autocommit = False
            cursor = raw_conn.cursor()

            try:
                cursor.execute("SET XACT_ABORT ON")

                # 1. DELETE existing proposal_base data
                yield _progress_line("Deleting existing proposal data", 38)
                cursor.execute("DELETE FROM dbo.proposal_base")
                deleted = cursor.rowcount
                print(f"[PROPOSAL UPLOAD] +{time.time()-t0:.1f}s  Deleted {deleted} rows"); sys.stdout.flush()

                # 2. Bulk INSERT via fast_executemany (NOCOUNT ON for speed)
                cursor.execute("SET NOCOUNT ON")
                cursor.fast_executemany = True
                BATCH = 20000
                inserted = 0
                total_batches = (len(rows_data) + BATCH - 1) // BATCH
                for start in range(0, len(rows_data), BATCH):
                    batch = rows_data[start:start + BATCH]
                    cursor.executemany(insert_sql, batch)
                    inserted += len(batch)
                    batch_num = start // BATCH + 1
                    batch_pct = 40 + int(35 * inserted / len(rows_data))
                    yield _progress_line(
                        f"Inserting rows: batch {batch_num}/{total_batches} ({inserted}/{len(rows_data)} rows)",
                        batch_pct
                    )
                    print(f"[PROPOSAL UPLOAD] +{time.time()-t0:.1f}s  Batch {batch_num}/{total_batches} ({inserted}/{len(rows_data)})"); sys.stdout.flush()
                cursor.execute("SET NOCOUNT OFF")

                # 3. Copy to history
                yield _progress_line("Copying proposal data to history table", 82)
                cursor.execute(history_sql, [uploaded_on_date])
                history_count = cursor.rowcount
                print(f"[PROPOSAL UPLOAD] +{time.time()-t0:.1f}s  Copied {history_count} rows to history"); sys.stdout.flush()

                # 4. Commit
                yield _progress_line("Committing transaction", 94)
                raw_conn.commit()
                print(f"[PROPOSAL UPLOAD] +{time.time()-t0:.1f}s  COMMITTED. Total time: {time.time()-t0:.1f}s"); sys.stdout.flush()

                _remove_cache(cache_key)
                try:
                    del request.session["_proposal_cache_key"]
                    del request.session["_proposal_uploaded_on"]
                    request.session.save()
                except Exception:
                    pass  # session cleanup is non-critical

                yield _progress_line("done", 100, result={
                    "success": True,
                    "deleted": deleted,
                    "inserted": inserted,
                    "history_inserted": history_count,
                    "message": (
                        f"Upload complete. {inserted} rows inserted into proposal_base, "
                        f"{history_count} rows copied to proposal_base_history."
                    ),
                })

            except Exception as e:
                raw_conn.rollback()
                traceback.print_exc()
                yield _progress_line("done", 100, result={"success": False, "error": f"Transaction rolled back: {str(e)}"})
            finally:
                raw_conn.autocommit = old_autocommit

        except Exception as e:
            traceback.print_exc()
            yield _progress_line("done", 100, result={"success": False, "error": str(e)})

    return StreamingHttpResponse(_stream(), content_type="text/plain")
