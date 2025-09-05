import json,random
from datetime import datetime

class AnalysisDataGenerator:
    def __init__(self):
        pass
    
    def create_analysis_data(self, 
                           analysis_date=None,
                           damage_function_id=1,
                           filters=None,
                           filter_type="AND",
                           events=None,
                           hand_drawn_event=None,
                           program=None,
                           include_shapes=False,
                           shapes_data=None,
                           include_date=True):
        # Create analysis list with single analysis
        analysis_data = {}
        
        # Only add date if both include_date is True AND analysis_date is provided
        if include_date and analysis_date:
            analysis_data['date'] = self._format_date(analysis_date)
        else:
            analysis_data['date'] = None
        
        data = {
            'analysisList': [analysis_data],
            'damageFunctionId': damage_function_id,
            'filterType': filter_type,
            'filters': filters or [],
            'events': events,
            'handDrawnEvent': hand_drawn_event,
            'program': program
        }
        
        # Add shapes filter if needed - need to structure it properly for the template
        if include_shapes and shapes_data:
            data['shapesFilter'] = shapes_data
        
        return data
    
    def _format_date(self, date_input):
        """Format date input into the expected structure"""
        if isinstance(date_input, datetime):
            return {
                'Day': date_input.day,
                'Month': date_input.month,
                'Year': date_input.year
            }
        elif isinstance(date_input, dict):
            return date_input
        else:
            # Assume it's a string and try to parse
            try:
                dt = datetime.strptime(str(date_input), '%Y-%m-%d')
                return {
                    'Day': dt.day,
                    'Month': dt.month,
                    'Year': dt.year
                }
            except:
                return None

    def create_filter(self, andor, asset_type, attribute, operator, value):
        """Helper method to create a filter dictionary"""
        return {
            'andor': andor,
            'assetType': asset_type,
            'attr': attribute,
            'op': operator,
            'value': value
        }
    
    def concentrics_filter_creation(self,radius_list,intensity_list):
        return [{
            "Radii": radius/1000,
            "Intensity": intensity,
            "Unit": "km",
            "unitType": {"name": "KiloMeters", "unit": "Km"}
        } for radius, intensity in zip(radius_list, intensity_list)]

    def create_shapes_filter(self, center_lat, center_lon, radius_list,intensity_list, shape_id=None):
        """Create shapes filter data structure"""
        return_shape_filter_list = []
        # print(f"Radius List: {radius_list}")
        # for radius_indivual in radius_list:
        # print(f"concentric: {self.concentrics_filter_creation(radius_list,intensity_list)}")
        return_shape_filter_list = {"features":[{
            'CenterLat': center_lat,
            'CenterLon': center_lon,
            'Concentrics': self.concentrics_filter_creation(radius_list,intensity_list),
            'Radii': 0,
            'properties': {
                'UserShapeType': 'Circle',
                'selectedUnit': 'Km',
                'shapeDrawnOnMap': 'circle',
                'shapeId': shape_id or '83751755073358253'
            },
            'type': 'Feature'
        }]}
        # print(f"Shape Filter List: {return_shape_filter_list}")
        return return_shape_filter_list

def template_creation_for_analysis(lat,long,radius_list,cause_of_loss_FilterList,intensity_list):
    generator = AnalysisDataGenerator()
    
    # print(f"Cause of Loss Filter List: {cause_of_loss_FilterList}")
    # Convert FilterItem objects to dicts for create_filter
    filters = [
        generator.create_filter(
            item.AndOr,
            item.AssetType,
            item.Attribute,
            item.Operator,
            item.Value
        ) for item in cause_of_loss_FilterList
    ]
    # filters = [
    #     generator.create_filter(cause_of_loss_FilterList[0]['AndOr'],cause_of_loss_FilterList[0]['AssetType'], cause_of_loss_FilterList[0]['Attribute'], cause_of_loss_FilterList[0]['Operator'], cause_of_loss_FilterList[0]['Value'])
    #     # generator.create_filter("AND","site","Cause Of Loss", "EQ", cause_of_loss)
    # ]
    shape_id = str(random.randint(10**16, 10**17 - 1))
    # Create shapes filter
    shapes = generator.create_shapes_filter(
        center_lat=lat,
        center_lon=long,
        radius_list=radius_list,
        intensity_list=intensity_list,
        shape_id=shape_id
    )
    # Create analysis data
    data = generator.create_analysis_data(
        # analysis_date={'Day': 1, 'Month': 1, 'Year': 2023},
        damage_function_id=1,
        filters=filters,
        filter_type="AND",
        include_shapes=True,
        shapes_data=shapes
    )
    # print("template data: ", data)
    return data
