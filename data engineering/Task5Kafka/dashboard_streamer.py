# Author: Yoo Xin Wei
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pyspark.sql import SparkSession
import time
import json

class StreamingDashboard:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("AirQualityDashboard") \
            .getOrCreate()
    
    def read_latest_data(self, hdfs_path):
        """Read latest streaming data from HDFS"""
        try:
            df = self.spark.read.json(hdfs_path)
            return df.toPandas()
        except:
            return pd.DataFrame()
    
    def create_dashboard(self):
        """Create Streamlit dashboard"""
        st.set_page_config(page_title="Air Quality Streaming Dashboard", layout="wide")
        
        st.title(" Real-time Air Quality Monitoring Dashboard")
        st.markdown("---")
        
        
        placeholder = st.empty()
        
        while True:
            with placeholder.container():
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    st.subheader("AQI Aggregation")
                    
                    
                with col2:
                    st.subheader("Hazardous Alerts")
                    
                    
                with col3:
                    st.subheader("Real-time Trends")
                    
                
                
                time.sleep(30)
                placeholder.empty()