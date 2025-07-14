"""
Data converter for transforming Fitbit data to Garmin-compatible formats.
Handles TCX, GPX, and FIT file generation for activities.
"""

import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any, Optional, Union
import gpxpy
import gpxpy.gpx
from xml.etree.ElementTree import Element, SubElement, tostring
from xml.dom import minidom

from .models import ActivityData, FitbitUserData, HeartRateData

logger = logging.getLogger(__name__)


class DataConverter:
    """Convert Fitbit data to Garmin-compatible formats."""
    
    def __init__(self, output_dir: Union[str, Path]):
        """Initialize converter with output directory."""
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"Initialized data converter with output directory: {self.output_dir}")
    
    def convert_activities_to_tcx(self, activities: List[ActivityData]) -> List[str]:
        """Convert activities to TCX format."""
        tcx_files = []
        
        for activity in activities:
            if activity.gps_data or activity.heart_rate_zones:
                tcx_file = self._generate_tcx_file(activity)
                if tcx_file:
                    tcx_files.append(tcx_file)
        
        logger.info(f"Generated {len(tcx_files)} TCX files")
        return tcx_files
    
    def convert_activities_to_gpx(self, activities: List[ActivityData]) -> List[str]:
        """Convert activities with GPS data to GPX format."""
        gpx_files = []
        
        for activity in activities:
            if activity.gps_data:
                gpx_file = self._generate_gpx_file(activity)
                if gpx_file:
                    gpx_files.append(gpx_file)
        
        logger.info(f"Generated {len(gpx_files)} GPX files")
        return gpx_files
    
    def _generate_tcx_file(self, activity: ActivityData) -> Optional[str]:
        """Generate a TCX file for a single activity."""
        try:
            # Create TCX root element
            tcx_root = Element('TrainingCenterDatabase')
            tcx_root.set('xmlns', 'http://www.garmin.com/xmlschemas/TrainingCenterDatabase/v2')
            tcx_root.set('xmlns:xsi', 'http://www.w3.org/2001/XMLSchema-instance')
            tcx_root.set('xsi:schemaLocation', 
                        'http://www.garmin.com/xmlschemas/TrainingCenterDatabase/v2 '
                        'http://www.garmin.com/xmlschemas/TrainingCenterDatabasev2.xsd')
            
            # Activities element
            activities_elem = SubElement(tcx_root, 'Activities')
            activity_elem = SubElement(activities_elem, 'Activity')
            activity_elem.set('Sport', self._map_activity_type_to_tcx(activity.activity_type))
            
            # Activity ID
            id_elem = SubElement(activity_elem, 'Id')
            id_elem.text = activity.start_time.strftime('%Y-%m-%dT%H:%M:%S.000Z')
            
            # Create a lap
            lap_elem = SubElement(activity_elem, 'Lap')
            lap_elem.set('StartTime', activity.start_time.strftime('%Y-%m-%dT%H:%M:%S.000Z'))
            
            # Lap totals
            total_time_elem = SubElement(lap_elem, 'TotalTimeSeconds')
            total_time_elem.text = str(activity.duration_ms / 1000)
            
            distance_elem = SubElement(lap_elem, 'DistanceMeters')
            distance_elem.text = str((activity.distance or 0) * 1000)  # Convert km to meters
            
            if activity.calories:
                calories_elem = SubElement(lap_elem, 'Calories')
                calories_elem.text = str(activity.calories)
            
            if activity.average_heart_rate:
                avg_hr_elem = SubElement(lap_elem, 'AverageHeartRateBpm')
                avg_hr_value = SubElement(avg_hr_elem, 'Value')
                avg_hr_value.text = str(activity.average_heart_rate)
            
            if activity.max_heart_rate:
                max_hr_elem = SubElement(lap_elem, 'MaximumHeartRateBpm')
                max_hr_value = SubElement(max_hr_elem, 'Value')
                max_hr_value.text = str(activity.max_heart_rate)
            
            # Intensity
            intensity_elem = SubElement(lap_elem, 'Intensity')
            intensity_elem.text = 'Active'
            
            # Track (for GPS data or time-based data)
            track_elem = SubElement(lap_elem, 'Track')
            
            # If we have GPS data, add trackpoints
            if activity.gps_data:
                self._add_gps_trackpoints(track_elem, activity)
            else:
                # Create basic trackpoints for time-based data
                self._add_time_trackpoints(track_elem, activity)
            
            # Generate filename
            filename = f"activity_{activity.log_id}_{activity.start_time.strftime('%Y%m%d_%H%M%S')}.tcx"
            filepath = self.output_dir / filename
            
            # Write TCX file
            rough_string = tostring(tcx_root, 'utf-8')
            reparsed = minidom.parseString(rough_string)
            pretty_xml = reparsed.toprettyxml(indent="  ")
            
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(pretty_xml)
            
            logger.info(f"Generated TCX file: {filepath}")
            return str(filepath)
            
        except Exception as e:
            logger.error(f"Error generating TCX file for activity {activity.log_id}: {e}")
            return None
    
    def _generate_gpx_file(self, activity: ActivityData) -> Optional[str]:
        """Generate a GPX file for a single activity with GPS data."""
        try:
            # Create GPX object
            gpx = gpxpy.gpx.GPX()
            
            # Create track
            gpx_track = gpxpy.gpx.GPXTrack()
            gpx_track.name = f"{activity.activity_name} - {activity.start_time.strftime('%Y-%m-%d %H:%M')}"
            gpx_track.type = activity.activity_type.value
            gpx.tracks.append(gpx_track)
            
            # Create track segment
            gpx_segment = gpxpy.gpx.GPXTrackSegment()
            gpx_track.segments.append(gpx_segment)
            
            # Add GPS points if available
            if activity.gps_data:
                for gps_point in activity.gps_data:
                    if isinstance(gps_point, dict) and 'latitude' in gps_point and 'longitude' in gps_point:
                        point = gpxpy.gpx.GPXTrackPoint(
                            latitude=gps_point['latitude'],
                            longitude=gps_point['longitude'],
                            elevation=gps_point.get('altitude'),
                            time=gps_point.get('time')
                        )
                        gpx_segment.points.append(point)
            
            # Generate filename
            filename = f"activity_{activity.log_id}_{activity.start_time.strftime('%Y%m%d_%H%M%S')}.gpx"
            filepath = self.output_dir / filename
            
            # Write GPX file
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(gpx.to_xml())
            
            logger.info(f"Generated GPX file: {filepath}")
            return str(filepath)
            
        except Exception as e:
            logger.error(f"Error generating GPX file for activity {activity.log_id}: {e}")
            return None
    
    def _add_gps_trackpoints(self, track_elem: Element, activity: ActivityData):
        """Add GPS trackpoints to TCX track element."""
        if not activity.gps_data:
            return
        
        for i, gps_point in enumerate(activity.gps_data):
            if not isinstance(gps_point, dict):
                continue
                
            trackpoint = SubElement(track_elem, 'Trackpoint')
            
            # Time
            time_elem = SubElement(trackpoint, 'Time')
            if 'time' in gps_point:
                time_elem.text = gps_point['time']
            else:
                # Estimate time based on duration and point index
                estimated_time = activity.start_time + timedelta(
                    seconds=(activity.duration_ms / 1000) * i / len(activity.gps_data)
                )
                time_elem.text = estimated_time.strftime('%Y-%m-%dT%H:%M:%S.000Z')
            
            # Position
            if 'latitude' in gps_point and 'longitude' in gps_point:
                position = SubElement(trackpoint, 'Position')
                lat_elem = SubElement(position, 'LatitudeDegrees')
                lat_elem.text = str(gps_point['latitude'])
                lon_elem = SubElement(position, 'LongitudeDegrees')
                lon_elem.text = str(gps_point['longitude'])
            
            # Altitude
            if 'altitude' in gps_point:
                alt_elem = SubElement(trackpoint, 'AltitudeMeters')
                alt_elem.text = str(gps_point['altitude'])
            
            # Heart rate (if available)
            if 'heart_rate' in gps_point:
                hr_elem = SubElement(trackpoint, 'HeartRateBpm')
                hr_value = SubElement(hr_elem, 'Value')
                hr_value.text = str(gps_point['heart_rate'])
    
    def _add_time_trackpoints(self, track_elem: Element, activity: ActivityData):
        """Add time-based trackpoints to TCX track element."""
        # Create trackpoints at regular intervals
        num_points = min(100, max(10, activity.duration_ms // 60000))  # 1 point per minute, max 100
        interval_ms = activity.duration_ms / num_points
        
        for i in range(num_points):
            trackpoint = SubElement(track_elem, 'Trackpoint')
            
            # Time
            time_elem = SubElement(trackpoint, 'Time')
            point_time = activity.start_time + timedelta(milliseconds=i * interval_ms)
            time_elem.text = point_time.strftime('%Y-%m-%dT%H:%M:%S.000Z')
            
            # Add heart rate if available
            if activity.average_heart_rate:
                hr_elem = SubElement(trackpoint, 'HeartRateBpm')
                hr_value = SubElement(hr_elem, 'Value')
                # Vary heart rate slightly around average
                hr_variation = int(activity.average_heart_rate * 0.1)  # 10% variation
                import random
                hr_value.text = str(activity.average_heart_rate + random.randint(-hr_variation, hr_variation))
    
    def _map_activity_type_to_tcx(self, activity_type) -> str:
        """Map our activity type to TCX sport type."""
        mapping = {
            'run': 'Running',
            'walk': 'Walking',
            'bike': 'Biking',
            'hike': 'Walking',
            'swim': 'Swimming',
            'workout': 'Other',
            'yoga': 'Other',
            'weights': 'Other',
            'sport': 'Other',
            'other': 'Other'
        }
        return mapping.get(activity_type.value, 'Other')
    
    def convert_heart_rate_to_fit(self, heart_rate_data: List[HeartRateData]) -> Optional[str]:
        """Convert heart rate data to FIT format (placeholder)."""
        # This would require the fit-tool library implementation
        # For now, return None as a placeholder
        logger.info("FIT file generation not yet implemented")
        return None
    
    def batch_convert_activities(self, user_data: FitbitUserData) -> Dict[str, List[str]]:
        """Convert all activities to multiple formats."""
        logger.info(f"Starting batch conversion of {len(user_data.activities)} activities")
        
        results = {
            'tcx_files': [],
            'gpx_files': [],
            'fit_files': []
        }
        
        # Convert to TCX
        if user_data.activities:
            print("  🏃 Converting activities to TCX format...")
            results['tcx_files'] = self.convert_activities_to_tcx(user_data.activities)
        
        # Convert to GPX (only GPS activities)
        gps_activities = [a for a in user_data.activities if a.gps_data]
        if gps_activities:
            print(f"  🗺️ Converting {len(gps_activities)} GPS activities to GPX format...")
            results['gpx_files'] = self.convert_activities_to_gpx(gps_activities)
        
        # FIT conversion (placeholder)
        # results['fit_files'] = self.convert_to_fit(user_data)
        
        logger.info(f"Batch conversion completed: {len(results['tcx_files'])} TCX, "
                   f"{len(results['gpx_files'])} GPX files")
        
        print(f"  ✅ Created {len(results['tcx_files'])} TCX files, {len(results['gpx_files'])} GPX files")
        
        return results