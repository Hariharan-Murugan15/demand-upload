"""
URL configuration for pipeline_insights project.

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/4.2/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.contrib import admin
from django.urls import path
from visualize import views
from django.conf.urls.static import static
from django.conf import settings

url_prefix = "GGMPipelineInsights/"
urlpatterns = [
    #  path('', views.home, name='home'),      
    # Default: when visiting /GGMPipelineInsights/ show the pipeline summary page
    path(url_prefix, views.pipeline_summary, name="pipeline_summary"),
    # Explicit dashboard route so clicking 'Dashboard' navigates to the dashboard page
    path(url_prefix + 'dashboard/', views.show_dashboard, name="show_dashboard"),
     path(url_prefix+'AjaxCallForDashBoard/', views.AjaxCallForDashBoard, name="AjaxCallForDashBoard"),
    #  path(url_prefix+'AjaxCallForIndexLabel/', views.AjaxCallForIndexLabel, name="AjaxCallForIndexLabel"),
     path(url_prefix + 'show_movement/', views.show_movement, name="show_movement"),
     path(url_prefix + 'winreport/', views.EmeaReport, name="winreport"),
     path(url_prefix + 'winreport/AjaxCallForQulUnQualtableData', views.AjaxCallForQulUnQualtableData, name="AjaxCallForQulUnQualtableData"),
     path(url_prefix + 'winreport/AjaxCallForWinstableData', views.AjaxCallForWinstableData, name="AjaxCallForWinstableData"),
     path(url_prefix + 'winreport/AjaxCallForAvgWinstableData', views.AjaxCallForAvgWinstableData, name="AjaxCallForAvgWinstableData"),
     path(url_prefix + 'show_details_table/', views.show_details_table, name="show_details_table"),
     path(url_prefix + 'show_plot/', views.show_plot, name="show_plot"),
     path(url_prefix + 'show_plot/AjaxCallForWonLost/', views.AjaxCallForWonLost, name="AjaxCallForWonLost"),
     path(url_prefix + 'pipeline_summary/', views.pipeline_summary, name="pipeline_summary"),
     path(url_prefix + 'pipeline_summary/AjaxCallForWonLostDeals/', views.AjaxCallForWonLostDeals, name="AjaxCallForWonLostDeals"),
     path(url_prefix + 'pipeline_summary/AjaxCallForTopDeals/', views.AjaxCallForTopDeals, name="AjaxCallForTopDeals"),
     path(url_prefix + 'pipeline_summary/AjaxCallForRecentCreatedDeals/', views.AjaxCallForRecentCreatedDeals, name="AjaxCallForRecentCreatedDeals"),
     path(url_prefix + 'pipeline_summary/AjaxCallForRecentLastModifiedDeals/', views.AjaxCallForRecentLastModifiedDeals, name="AjaxCallForRecentLastModifiedDeals"),
     path(url_prefix+'UploadData/', views.show_UploadData, name="show_UploadData"),
     path(url_prefix + 'download_top_deals_excel/', views.download_top_deals_excel, name='download_top_deals_excel'),
     path(url_prefix + 'AjaxGetWinzoneDetails/', views.AjaxGetWinzoneDetails, name='AjaxGetWinzoneDetails'),
     path(url_prefix + 'pipeline_vs_demand/', views.pipeline_vs_demand, name='pipeline_vs_demand'),
     path(url_prefix + 'pipeline_vs_demand/AjaxCallForPipelineVsDemand/', views.AjaxCallForPipelineVsDemand, name='AjaxCallForPipelineVsDemand'),
     path(url_prefix + 'pipeline_vs_demand/AjaxCallForPVDStats/', views.AjaxCallForPVDStats, name='AjaxCallForPVDStats'),
     path(url_prefix + 'pipeline_vs_demand/AjaxCallForQualifiedPipelineZeroDemand/', views.AjaxCallForQualifiedPipelineZeroDemand, name='AjaxCallForQualifiedPipelineZeroDemand'),
     path(url_prefix + 'pipeline_vs_demand/AjaxCallForDemandsChart/', views.AjaxCallForDemandsChart, name='AjaxCallForDemandsChart'),
     path(url_prefix + 'pipeline_vs_demand/AjaxCallForDemandsByAccountName/', views.AjaxCallForDemandsByAccountName, name='AjaxCallForDemandsByAccountName'),
     path(url_prefix + 'pipeline_vs_demand/AjaxCallForDemandsReqStartDate/', views.AjaxCallForDemandsReqStartDate, name='AjaxCallForDemandsReqStartDate'),
     path(url_prefix + 'pipeline_vs_demand/AjaxCallForSunburstData/', views.AjaxCallForSunburstData, name='AjaxCallForSunburstData'),

     # Demand Upload wizard
     path(url_prefix + 'demand_upload/', views.demand_upload_page, name='demand_upload_page'),
     path(url_prefix + 'demand_upload/upload_file/', views.demand_upload_file, name='demand_upload_file'),
     path(url_prefix + 'demand_upload/preview/', views.demand_upload_preview, name='demand_upload_preview'),
     path(url_prefix + 'demand_upload/execute/', views.demand_upload_execute, name='demand_upload_execute'),

]+ static(settings.STATIC_URL, document_root=settings.STATIC_ROOT)




