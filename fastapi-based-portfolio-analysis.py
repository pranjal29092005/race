from dotenv import load_dotenv
load_dotenv('.env')

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
import os
import json
import time
import psycopg2
from typing import List, Dict, Any, Iterable, Optional, Union
from itertools import chain
import numbers
import uvicorn
import re
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("portfolio-analysis")

# Add file handler for gunicorn.log
file_handler = logging.FileHandler("gunicorn.log")
file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
logger.addHandler(file_handler)

# Import your existing modules
from race_server import RaceServer
from portfolio_analysis import run_pipeline
from template_utils import template_creation_for_analysis
from pi_utils import stream_parquet_rows

app = FastAPI(
    title="Portfolio Risk Analysis API",
    description="API for portfolio risk analysis with geographical exposure assessment",
    version="1.0.0"
)

# New models for Portfolio Impact Analysis
class FilterItem(BaseModel):
    AndOr: str = "AND"
    AssetType: Optional[str] = None
    Attribute: str
    Operator: str
    Value: str

class FilterSet(BaseModel):
    AndOr: str = "AND"
    FilterList: List[FilterItem]

class ExposureFilterSets(BaseModel):
    AssetModel: str
    FilterList: List[FilterSet]

class PortfolioImpactAnalysisConfig(BaseModel):
    AdditionalMeasure: List[str]
    Apply_As_All_Perils: bool = Field(alias="Apply As All Perils", default=False)
    Count: int
    CurrencyCode: str
    DamageFactors: List[float]
    Exposure_Id_Base: int = Field(alias="Exposure Id Base")
    Exposure_Id_Account: int = Field(alias="Exposure Id Account")
    ExposureFilterSets: ExposureFilterSets
    IgnoreOutOfBoundAsset: bool
    IncludeAllContracts: bool
    ObjectSubType: str
    SortDirection: str
    Measure: str
    damageAdjustment: float
    deltaOffset: int
    eventRadii: List[int]
    operator: str
    peril: str
    quantile: int
    subPeril: str
    threshold: float
    Results_Key: str = Field(alias="Results Key")

class PortfolioImpactAnalysisRequest(BaseModel):
    PortfolioImpactAnalysis: PortfolioImpactAnalysisConfig

# Legacy request model (keep for backward compatibility)
class AnalysisRequest(BaseModel):
    exposureIdA: int
    exposureIdB: int
    radius: str
    causeOfLoss: str

# Response models
class LocationAnalysisResult(BaseModel):
    latitude: float
    longitude: float
    exposure_summary: Dict[str, Any]

class AnalysisResponse(BaseModel):
    portfolio_merged_exposure_summary: List[LocationAnalysisResult]
    total_processing_time: float
    total_locations_analyzed: int
    status: str

class PortfolioImpactAnalysisResponse(BaseModel):
    results: Dict[str, Any]
    total_processing_time: float
    status: str
    analysis_config: Dict[str, Any]

# Updated models for the new input format
class ExposureBase(BaseModel):
    Id: int
    ObjectSubType: str

class ExposureAccount(BaseModel):
    Id: int
    ObjectSubType: str

class ExposureConfig(BaseModel):
    Base: ExposureBase
    Account: ExposureAccount

class PortfolioImpactConfig(BaseModel):
    Peril: str
    SubPeril: str
    EventRadii: List[int]
    DamageFactors: List[float]
    TopNBy: str
    SortDirection: str
    Count: int
    Threshold: float
    Operator: str
    AdditionalMeasures: List[str]
    HeatMap: bool
    S3Bucket: str
    S3Key: str

class NewPortfolioImpactAnalysisRequest(BaseModel):
    Exposure: ExposureConfig
    IgnoreOutOfBoundAsset: bool
    IncludeAllContracts: bool
    ApplyAsAllPerils: bool
    CurrencyCode: str
    DamageAdjustment: float
    Quantile: int
    ExposureFilterSets: ExposureFilterSets
    PortfolioImpact: PortfolioImpactConfig
    Results_Key: str = Field(alias="Results Key")

# Updated response models for the analysis results
class AssetAnalysisResult(BaseModel):
    Asset_Name: str = Field(alias="Asset Name")
    Latitude: float
    Longitude: float
    Asset_Number: int = Field(alias="Asset Number") 
    Asset_Schedule_Name: int = Field(alias="Asset Schedule Name")
    Contract_Number: str = Field(alias="Contract Number")
    Base: Dict[str, Any]
    Account: Dict[str, Any]
    Combined: Dict[str, Any]

    class Config:
        populate_by_name = True

class NewAnalysisResponse(BaseModel):
    Results_Key: str = Field(alias="Results Key")
    Value: List[AssetAnalysisResult]

    class Config:
        populate_by_name = True

# Global configuration - loaded once at startup
config = None
cur_dir = os.path.dirname(os.path.abspath(__file__))

def load_config():
    """Load configuration from JSON file"""
    global config
    config_json = os.path.join(cur_dir, 'db_config.json')
    try:
        with open(config_json, 'r') as f:
            config = json.load(f)
        logger.info(f"Configuration loaded successfully from {config_json}")
        return config
    except Exception as e:
        logger.error(f"Failed to load configuration file {config_json}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Failed to load configuration: {str(e)}")

def get_db_conn():
    """Create database connection"""
    if not config:
        logger.error("Configuration not loaded before DB connection attempt")
        raise HTTPException(status_code=500, detail="Configuration not loaded")
    
    working_env = os.getenv("WORKING_ENV", "prod")
    db_info = config.get(working_env)
    if not db_info:
        logger.error(f"No DB info found for environment: {working_env}")
        raise HTTPException(status_code=500, detail=f"No DB info for environment: {working_env}")
    
    try:
        conn_str = 'host={} dbname={} user={} password={} port={}'.format(
            db_info['host'], db_info['database'], db_info['user'], db_info['pw'], db_info['port']
        )
        logger.info(f"Connecting to database host: {db_info['host']}, database: {db_info['database']}")
        return psycopg2.connect(conn_str)
    except Exception as e:
        logger.error(f"Failed to connect to database: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Database connection failed: {str(e)}")

def get_lat_long_from_portfolio(portfolio_id: int):
    try:
        logger.info(f"Extracting geographical data from portfolio {portfolio_id}...")
        working_env = os.getenv("WORKING_ENV", "prod")
        output_parquet_path, contract_number = run_pipeline(
            portfolio_id=portfolio_id, 
            working_env=working_env
        )
        logger.info(f"Pipeline completed. Output path: {output_parquet_path}, Contract Number: {contract_number}")
        
        lat_long_pq_table = stream_parquet_rows(
            output_parquet_path,
            columns=["asset_name", "asset_number", "latitude", "longitude","asset_schedule_id"],
            rename=None,
            as_dict=True
        )
        # logger.info(f"Extracted {len(lat_long_pq_table)} geographical data points from portfolio {portfolio_id}")
        return lat_long_pq_table, contract_number
        
    except Exception as e:
        logger.error(f"Error extracting coordinates from portfolio {portfolio_id}: {str(e)}")
        raise HTTPException(
            status_code=500, 
            detail=f"Failed to extract coordinates from portfolio {portfolio_id}: {str(e)}"
        )

def merge_exposure_summaries(
    *datasets: Iterable[Dict[str, Any]],
    precision: int = 6,
    keep_meta: bool = False
) -> List[Dict[str, Any]]:
    try:
        logger.info(f"Merging exposure summaries with precision {precision}, keep_meta: {keep_meta}")
        combined: Dict[tuple, Dict[str, Any]] = {}
        rnd = round
        is_num = lambda x: isinstance(x, numbers.Real)

        for entry in chain.from_iterable(datasets):
            lat = entry.get("latitude")
            lon = entry.get("longitude")
            if lat is None or lon is None:
                continue

            key = (rnd(lat, precision), rnd(lon, precision))
            bucket = combined.get(key)
            if bucket is None:
                bucket = {
                    "exposure_summary": {},
                    "latitude": lat,
                    "longitude": lon,
                }
                if keep_meta:
                    bucket["_portfolio_ids"] = set()
                    bucket["_sources"] = set()
                combined[key] = bucket

            es = entry.get("exposure_summary")
            if es is None:
                es = entry.get("exposure-summary", {})

            if es:
                out_es = bucket["exposure_summary"]
                for k, v in es.items():
                    if is_num(v):
                        out_es[k] = out_es.get(k, 0.0) + float(v)

            if keep_meta:
                pid = entry.get("portfolio_id")
                src = entry.get("portfolio_source")
                if pid is not None:
                    bucket["_portfolio_ids"].add(pid)
                if src is not None:
                    bucket["_sources"].add(src)

        result: List[Dict[str, Any]] = []
        for b in combined.values():
            if keep_meta:
                b["portfolio_ids"] = sorted(b.pop("_portfolio_ids"))
                b["portfolio_sources"] = sorted(b.pop("_sources"))
            result.append(b)

        logger.info(f"Successfully merged exposure summaries, result count: {len(result)}")
        return result
    except Exception as e:
        logger.error(f"Error merging exposure summaries: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error merging exposure summaries: {str(e)}")

def process_all_locations(portfolio_id: int, lat_long_data: List[Dict], radius: int, cause_of_loss: str, portfolio_source: str, race_ip: str):
    try:
        logger.info(f"Processing {len(lat_long_data)} locations for portfolio {portfolio_id} with source {portfolio_source}")
        user_name = os.getenv("user","tmh.test@eigenrisk.com")
        race_server = RaceServer(user=user_name, ip=race_ip)
        race_server.create_session()

        portfolio_data = {'exposures': [{'id': portfolio_id, 'portfolio': True}]}
        portfolio_loading = race_server.load_portfolio(portfolio_data)
        portfolio_value = portfolio_loading.get("Value", 0)
        logger.info(f"Portfolio {portfolio_id} loaded with value: {portfolio_value} from source {portfolio_source}")
        exposure_summary_result = []

        for lat_long_json_obj in lat_long_data:
            lat = lat_long_json_obj['latitude']
            long = lat_long_json_obj['longitude']
            asset_name = lat_long_json_obj['asset_name']
            asset_number = lat_long_json_obj['asset_number']

            logger.info(f"Analyzing Portfolio ({portfolio_id}) at asset number {asset_number} with {asset_name}...")
            try:
                data_for_analysis = template_creation_for_analysis(
                        lat, long, 
                        radius=radius,
                        cause_of_loss=cause_of_loss
                )

                set_analysis_call = race_server.run_analysis(data_for_analysis)
                exposures_summary_call = race_server.get_exposure_summary()
                exposures_summary_value = exposures_summary_call.get("Value", 0)

                exposure_summary_result.append({
                    'portfolio_id': portfolio_id,
                    'exposure_summary': exposures_summary_value.get('Analysis0', {}),
                    'portfolio_source': portfolio_source,
                    'latitude': lat,
                    'longitude': long
                })
                
            except Exception as e:
                logger.error(f"Error analyzing Portfolio {portfolio_source} asset {asset_number}: {str(e)}")
                exposure_summary_result.append({
                    'portfolio_id': portfolio_id,
                    'portfolio_value': 0,
                    'exposure_summary': {},
                    'portfolio_source': portfolio_source,
                    'error': str(e)
                })

        logger.info(f"Completed processing {len(exposure_summary_result)} locations for portfolio {portfolio_id}")
        return exposure_summary_result
    except Exception as e:
        logger.error(f"Error in process_all_locations for portfolio {portfolio_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error in process_all_locations: {str(e)}")

@app.on_event("startup")
async def startup_event():
    """Load configuration on startup"""
    try:
        load_config()
        logger.info("Configuration loaded successfully on startup")
    except Exception as e:
        logger.error(f"Error during startup configuration load: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Startup configuration load failed: {str(e)}")

def merge_exposure_summaries_by_asset(
    base_results: List[Dict[str, Any]],
    account_results: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    try:
        logger.info(f"Merging exposure summaries by asset - Base: {len(base_results)}, Account: {len(account_results)}")
        base_lookup = {result.get('asset_number'): result for result in base_results}
        account_lookup = {result.get('asset_number'): result for result in account_results}
        
        all_asset_numbers = set(base_lookup.keys()) | set(account_lookup.keys())
        logger.info(f"Total unique assets to merge: {len(all_asset_numbers)}")
        
        merged_results = []
        
        for asset_number in all_asset_numbers:
            base_data = base_lookup.get(asset_number, {})
            account_data = account_lookup.get(asset_number, {})
            
            asset_name = base_data.get('asset_name') or account_data.get('asset_name', f"{asset_number} - {asset_number}")
            asset_schedule_name = base_data.get('asset_schedule_name') or account_data.get('asset_schedule_name', asset_number)
            contract_number = base_data.get('contract_number') or account_data.get('contract_number', f"{asset_number} - {asset_number}")
            latitude = base_data.get('latitude') or account_data.get('latitude', 0.0)
            longitude = base_data.get('longitude') or account_data.get('longitude', 0.0)
            
            base_exposure = base_data.get('exposure_summary', {})
            account_exposure = account_data.get('exposure_summary', {})
            
            combined_exposure = {}
            all_keys = set(base_exposure.keys()) | set(account_exposure.keys())
            
            for key in all_keys:
                base_val = base_exposure.get(key, 0)
                account_val = account_exposure.get(key, 0)
                
                if isinstance(base_val, (int, float)) and isinstance(account_val, (int, float)):
                    combined_exposure[key] = base_val + account_val
                else:
                    combined_exposure[key] = base_val or account_val
            
            merged_result = {
                "Asset Name": asset_name,
                "Latitude": latitude,
                "Longitude": longitude,
                "Asset Number": asset_number,
                "Asset Schedule Name": asset_schedule_name,
                "Contract Number": contract_number,
                "Base": base_exposure,
                "Account": account_exposure,
                "Combined": combined_exposure
            }
            
            merged_results.append(merged_result)
        
        logger.info(f"Successfully merged {len(merged_results)} asset results")
        return merged_results
    except Exception as e:
        logger.error(f"Error merging exposure summaries by asset: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error merging exposure summaries: {str(e)}")

def process_all_locations_enhanced(portfolio_id: int, lat_long_data: List[Dict], radius: List[int], intensity_list: List[int], additional_measures: List[str], cause_of_loss_FilterList: List[Dict], portfolio_source: str, race_ip: str, contract_number: str):
    try:
        logger.info(f"Enhanced processing for portfolio {portfolio_id}, source: {portfolio_source}")
        user_name = os.getenv("user","tmh.test@eigenrisk.com")
        race_server = RaceServer(user=user_name, ip=race_ip)
        race_server.create_session()

        portfolio_data = {'exposures': [{'id': portfolio_id, 'portfolio': True}]}
        portfolio_loading = race_server.load_portfolio(portfolio_data)
        portfolio_value = portfolio_loading.get("Value", 0)
        logger.info(f"Portfolio {portfolio_id} loaded with value: {portfolio_value} from source {portfolio_source}")
        
        exposure_summary_result = []

        for lat_long_json_obj in lat_long_data:
            lat = lat_long_json_obj['latitude']
            long = lat_long_json_obj['longitude']
            asset_name = lat_long_json_obj['asset_name']
            asset_number = lat_long_json_obj['asset_number']
            asset_sch_name = lat_long_json_obj['asset_schedule_id']
            
            logger.info(f"Analyzing Portfolio ({portfolio_id}) at asset number {asset_number} with {asset_name}...")
            
            try:
                data_for_analysis = template_creation_for_analysis(
                    lat, long, 
                    radius_list=radius,
                    cause_of_loss_FilterList=cause_of_loss_FilterList,
                    intensity_list=intensity_list
                )

                set_analysis_call = race_server.run_analysis(data_for_analysis)
                exposures_summary_call = race_server.get_exposure_summary(additional_measures=additional_measures)
                exposures_summary_value = exposures_summary_call.get("Value", {})

                exposure_summary = exposures_summary_value.get('Analysis0', {})
                exposure_summary["Disaggregated Asset"] = False

                exposure_summary_result.append({
                    'portfolio_id': portfolio_id,
                    'asset_name': asset_name,
                    'asset_number': asset_number,
                    'asset_schedule_name': asset_sch_name,
                    "contract_number": contract_number,
                    'exposure_summary': exposure_summary,
                    'portfolio_source': portfolio_source,
                    'latitude': lat,
                    'longitude': long
                })
                
            except Exception as e:
                logger.error(f"Error analyzing Portfolio {portfolio_source} asset {asset_number}: {str(e)}")
                exposure_summary_result.append({
                    'portfolio_id': portfolio_id,
                    'asset_name': asset_name,
                    'asset_number': asset_number,
                    'asset_schedule_name': asset_number,
                    'contract_number': f"{asset_number} - {asset_number}",
                    'exposure_summary': {},
                    'portfolio_source': portfolio_source,
                    'latitude': lat,
                    'longitude': long,
                    'error': str(e)
                })

        logger.info(f"Enhanced processing completed for portfolio {portfolio_id}, processed {len(exposure_summary_result)} assets")
        return exposure_summary_result
    except Exception as e:
        logger.error(f"Error in process_all_locations_enhanced for portfolio {portfolio_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error in process_all_locations_enhanced: {str(e)}")

def filter_and_sort_asset_results(
    merged_results: List[Dict[str, Any]],
    measure: str,
    operator: str,
    sorting_direction: str,
    return_count: int,
    threshold: Optional[float] = None,
    section: str = "Combined",  
) -> List[Dict[str, Any]]:
    try:
        logger.info(f"Filtering results for measure: {measure}, operator: {operator}, direction: {sorting_direction}, threshold: {threshold}, section: {section}")
        
        op_map = {
            'GT': lambda x, y: x > y,
            'GE': lambda x, y: x >= y,
            'LT': lambda x, y: x < y,
            'LE': lambda x, y: x <= y,
            'EQ': lambda x, y: x == y,
            'NE': lambda x, y: x != y,
        }
        
        if threshold is None:
            threshold = 0.0

        filtered = [
            result for result in merged_results
            if measure in result.get(section, {})
            and op_map.get(operator, lambda x, y: True)(result[section][measure], threshold)
        ]

        reverse = sorting_direction.lower() == 'descending'
        sorted_results = sorted(
            filtered,
            key=lambda x: x.get(section, {}).get(measure, 0.0),
            reverse=reverse
        )
        if return_count > 0:
            sorted_results = sorted_results[:return_count]
        
        logger.info(f"Filtering complete: {len(filtered)} filtered, {len(sorted_results)} returned")
        return sorted_results
    except Exception as e:
        logger.error(f"Error filtering and sorting asset results: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error filtering and sorting asset results: {str(e)}")

@app.get("/health-check")
async def health_check():
    """Health check endpoint"""
    logger.info("Health check requested")
    return {"status": "healthy", "message": "Portfolio Analysis API is running"}

@app.get("/")
async def root():
    """Root endpoint with API information"""
    logger.info("Root endpoint accessed")
    return {
        "message": "Portfolio Risk Analysis API",
        "version": "1.0.0",
        "endpoints": {
            "POST /analyze-portfolio": "Run portfolio risk analysis",
            "GET /health": "Health check",
            "GET /docs": "API documentation"
        }
    }

@app.post("/analyze-portfolio", response_model=NewAnalysisResponse)
async def analyze_portfolio(request: NewPortfolioImpactAnalysisRequest):
    start_time = time.time()
    try:
        logger.info(f"Starting portfolio analysis - Base: {request.Exposure.Base.Id}, Account: {request.Exposure.Account.Id}")
        
        lat_long_coordinates, contract_number = get_lat_long_from_portfolio(request.Exposure.Account.Id)

        race_ip = os.getenv("RACE_IP", "127.0.0.1")
        logger.info(f"Using RACE IP: {race_ip}")
        
        intensity_list = request.PortfolioImpact.DamageFactors
        measures_for_exposure_summary = request.PortfolioImpact.AdditionalMeasures
        
        cause_of_loss_FilterList = []
        cause_of_loss = None
        for filter_set in request.ExposureFilterSets.FilterList:
            cause_of_loss_FilterList = filter_set.FilterList
            for filter_item in filter_set.FilterList:
                if filter_item.Attribute == "Cause Of Loss":
                    cause_of_loss = filter_item.Value
                    break
        
        logger.info(f"Cause of Loss extracted: {cause_of_loss}")
        
        exposure_base_result = process_all_locations_enhanced(
            portfolio_id=request.Exposure.Base.Id,
            lat_long_data=lat_long_coordinates,
            radius=request.PortfolioImpact.EventRadii,
            intensity_list=intensity_list,
            additional_measures=measures_for_exposure_summary,
            cause_of_loss_FilterList=cause_of_loss_FilterList,
            portfolio_source='Base',
            race_ip=race_ip,
            contract_number=contract_number
        )

        lat_long_coordinates_v2, _ = get_lat_long_from_portfolio(request.Exposure.Account.Id)
        exposure_account_result = process_all_locations_enhanced(
            portfolio_id=request.Exposure.Account.Id,
            lat_long_data=lat_long_coordinates_v2,
            radius=request.PortfolioImpact.EventRadii,
            intensity_list=intensity_list,
            additional_measures=measures_for_exposure_summary,
            cause_of_loss_FilterList=cause_of_loss_FilterList,
            portfolio_source='Account',
            race_ip=race_ip,
            contract_number=contract_number
        )

        merged_results = merge_exposure_summaries_by_asset(exposure_base_result, exposure_account_result)
        logger.info("Analysis completed successfully for both portfolios at all locations")
        
        filtered_results = filter_and_sort_asset_results(
            merged_results,
            measure=request.PortfolioImpact.TopNBy,
            operator=request.PortfolioImpact.Operator,
            sorting_direction=request.PortfolioImpact.SortDirection,
            return_count=request.PortfolioImpact.Count,
            threshold=request.PortfolioImpact.Threshold,
            section="Combined"
        )

        if request.PortfolioImpact.Count > 0:
            filtered_results = filtered_results[:request.PortfolioImpact.Count]

        response_data = {
            "Results Key": request.Results_Key,
            "Value": filtered_results
        }

        total_time = time.time() - start_time
        logger.info(f"Portfolio analysis completed successfully in {total_time:.2f} seconds")
        return response_data
        
    except Exception as e:
        logger.error(f"Error during combined portfolio analysis: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Combined portfolio analysis failed: {str(e)}"
        )

if __name__ == "__main__":
    logger.info("Starting FastAPI server...")
    uvicorn.run(app, host="0.0.0.0", port=8000)