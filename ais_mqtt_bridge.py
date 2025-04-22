#!/usr/bin/env python3

import socket
import paho.mqtt.client as mqtt
import logging
import re
import json
from typing import Dict, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# MQTT Configuration
MQTT_BROKER = "test.mosquitto.org"
MQTT_PORT = 1883
MQTT_TOPIC = "ais/data"

class AISMQTTBridge:
    def __init__(self):
        self.udp_socket = None
        self.mqtt_client = None

    def setup_udp_socket(self):
        """Set up UDP socket for receiving AIS data"""
        try:
            self.udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.udp_socket.bind(('127.0.0.1', 9999))
            logger.info("UDP socket bound to 127.0.0.1:9999")
        except Exception as e:
            logger.error(f"Failed to set up UDP socket: {e}")
            raise

    def setup_mqtt(self):
        """Set up MQTT client"""
        try:
            self.mqtt_client = mqtt.Client()
            self.mqtt_client.connect(MQTT_BROKER, MQTT_PORT)
            logger.info(f"Connected to MQTT broker at {MQTT_BROKER}:{MQTT_PORT}")
        except Exception as e:
            logger.error(f"Failed to connect to MQTT broker: {e}")
            raise

    def decode_payload(self, payload: str) -> Optional[Dict]:
        """Decode AIS payload from 6-bit ASCII to actual data"""
        try:
            # First convert 6-bit ASCII to binary
            binary = ''
            for char in payload:
                # Convert each character to its 6-bit value
                ord_c = ord(char) - 48
                if ord_c > 40:
                    ord_c -= 8
                # Convert to 6-bit binary string
                binary += format(ord_c, '06b')

            # Get message type (first 6 bits)
            msg_type = int(binary[0:6], 2)

            decoded = {
                'msg_type': msg_type
            }

            # Decode based on message type
            if msg_type in [1, 2, 3]:  # Position Report Class A
                # Convert raw longitude (28 bits) to decimal degrees
                raw_lon = int(binary[61:89], 2)
                lon = raw_lon / 600000.0
                if raw_lon >= 0x8000000:  # Check if negative (bit 28 is set)
                    lon = -(0x10000000 - raw_lon) / 600000.0
                
                # Convert raw latitude (27 bits) to decimal degrees
                raw_lat = int(binary[89:116], 2)
                lat = raw_lat / 600000.0
                if raw_lat >= 0x4000000:  # Check if negative (bit 27 is set)
                    lat = -(0x8000000 - raw_lat) / 600000.0

                decoded.update({
                    'repeat': int(binary[6:8], 2),
                    'mmsi': int(binary[8:38], 2),
                    'nav_status': int(binary[38:42], 2),
                    'rot': int(binary[42:50], 2),  # Rate of Turn
                    'sog': int(binary[50:60], 2) / 10.0,  # Speed Over Ground
                    'pos_accuracy': bool(int(binary[60:61], 2)),
                    'longitude': lon,
                    'latitude': lat,
                    'cog': int(binary[116:128], 2) / 10.0,  # Course Over Ground
                    'true_heading': int(binary[128:137], 2),
                    'timestamp': int(binary[137:143], 2),
                })
            elif msg_type == 5:  # Static and Voyage Related Data
                decoded.update({
                    'repeat': int(binary[6:8], 2),
                    'mmsi': int(binary[8:38], 2),
                    'ais_version': int(binary[38:40], 2),
                    'imo': int(binary[40:70], 2),
                    'callsign': self.decode_string(binary[70:112]),
                    'shipname': self.decode_string(binary[112:232]),
                    'ship_type': int(binary[232:240], 2),
                    'to_bow': int(binary[240:249], 2),
                    'to_stern': int(binary[249:258], 2),
                    'to_port': int(binary[258:264], 2),
                    'to_starboard': int(binary[264:270], 2),
                })
            elif msg_type == 18:  # Standard Class B Position Report
                # Convert raw longitude (28 bits) to decimal degrees
                raw_lon = int(binary[57:85], 2)
                lon = raw_lon / 600000.0
                if raw_lon >= 0x8000000:  # Check if negative (bit 28 is set)
                    lon = -(0x10000000 - raw_lon) / 600000.0
                
                # Convert raw latitude (27 bits) to decimal degrees
                raw_lat = int(binary[85:112], 2)
                lat = raw_lat / 600000.0
                if raw_lat >= 0x4000000:  # Check if negative (bit 27 is set)
                    lat = -(0x8000000 - raw_lat) / 600000.0

                decoded.update({
                    'repeat': int(binary[6:8], 2),
                    'mmsi': int(binary[8:38], 2),
                    'sog': int(binary[46:56], 2) / 10.0,
                    'pos_accuracy': bool(int(binary[56:57], 2)),
                    'longitude': lon,
                    'latitude': lat,
                    'cog': int(binary[112:124], 2) / 10.0,
                    'true_heading': int(binary[124:133], 2),
                    'timestamp': int(binary[133:139], 2),
                })
            elif msg_type == 24:  # Static Data Report
                decoded.update({
                    'repeat': int(binary[6:8], 2),
                    'mmsi': int(binary[8:38], 2),
                    'part_no': int(binary[38:40], 2)
                })
                if decoded['part_no'] == 0:
                    decoded['shipname'] = self.decode_string(binary[40:160])
                elif decoded['part_no'] == 1:
                    decoded.update({
                        'ship_type': int(binary[40:48], 2),
                        'vendor_id': self.decode_string(binary[48:90]),
                        'callsign': self.decode_string(binary[90:132]),
                        'to_bow': int(binary[132:141], 2),
                        'to_stern': int(binary[141:150], 2),
                        'to_port': int(binary[150:156], 2),
                        'to_starboard': int(binary[156:162], 2),
                    })

            return decoded
        except Exception as e:
            logger.error(f"Error decoding payload: {e}")
            return None

    def decode_string(self, binary: str) -> str:
        """Decode 6-bit ASCII string from binary"""
        result = ""
        for i in range(0, len(binary), 6):
            char_bin = binary[i:i+6]
            if char_bin == "000000":
                break
            char_val = int(char_bin, 2)
            if char_val < 32:
                char_val += 64
            result += chr(char_val)
        return result.strip("@").strip()

    def parse_nmea(self, message):
        """Parse AIS (AIVDM/AIVDO) message"""
        try:
            # Basic AIS message validation
            if not message.startswith('!'):
                raise ValueError("Invalid AIS message format - must start with !")
            
            # Split message and checksum
            message_parts = message.split('*')
            if len(message_parts) != 2:
                raise ValueError("Invalid AIS message format - missing checksum")
            
            message_body, checksum = message_parts
            
            # Split the message into fields
            fields = message_body.split(',')
            if len(fields) < 7:
                raise ValueError("Invalid AIS message format - insufficient fields")
            
            # Parse into structured format
            parsed = {
                'message_type': fields[0],          # !AIVDM or !AIVDO
                'fragment_count': int(fields[1]),   # Number of fragments
                'fragment_number': int(fields[2]),  # Fragment number
                'message_id': fields[3],            # Sequential message ID
                'channel': fields[4],               # AIS Channel
                'payload': fields[5],               # Encoded AIS data
                'fill_bits': int(fields[6]) if fields[6] else 0,  # Number of fill bits
                'checksum': checksum
            }

            # Decode the payload
            decoded_data = self.decode_payload(parsed['payload'])
            if decoded_data:
                parsed['decoded'] = decoded_data
            
            return parsed
        except Exception as e:
            logger.error(f"Error parsing AIS message: {e}")
            return None

    def process_message(self, data):
        """Process AIS message and publish to MQTT"""
        try:
            # Decode and parse message
            message = data.decode('utf-8').strip()
            parsed_data = self.parse_nmea(message)
            
            if parsed_data:
                # Log the processed message
                logger.info(f"Processed AIS message: {parsed_data}")
                
                # Publish to MQTT as JSON
                mqtt_payload = json.dumps(parsed_data)
                self.mqtt_client.publish(MQTT_TOPIC, mqtt_payload)
                logger.debug(f"Published to MQTT topic {MQTT_TOPIC}: {mqtt_payload}")
            else:
                logger.warning(f"Failed to parse message: {message}")
            
        except Exception as e:
            logger.error(f"Error processing message: {e}")

    def run(self):
        """Main loop for receiving and processing messages"""
        try:
            self.setup_udp_socket()
            self.setup_mqtt()
            
            logger.info("Starting AIS to MQTT bridge...")
            
            while True:
                try:
                    data, addr = self.udp_socket.recvfrom(1024)
                    self.process_message(data)
                except Exception as e:
                    logger.error(f"Error in main loop: {e}")
                    continue
                    
        except KeyboardInterrupt:
            logger.info("Shutting down...")
        finally:
            if self.udp_socket:
                self.udp_socket.close()
            if self.mqtt_client:
                self.mqtt_client.disconnect()

if __name__ == "__main__":
    bridge = AISMQTTBridge()
    bridge.run() 