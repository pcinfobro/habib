#!/usr/bin/env python3
"""
Security Configuration for PowerBI Service
=========================================
Handles security settings and authentication
"""

import os
import json
import logging
from datetime import datetime, timedelta

class SecurityConfig:
    """Security configuration manager"""
    
    def __init__(self):
        self.config = {
            'encryption_enabled': True,
            'token_expiry': 3600,  # 1 hour
            'max_attempts': 3,
            'lockout_duration': 300,  # 5 minutes
        }
    
    def get_powerbi_credentials(self):
        """Get PowerBI credentials from environment"""
        return {
            'client_id': os.getenv('POWERBI_CLIENT_ID', 'demo-client-id'),
            'client_secret': os.getenv('POWERBI_CLIENT_SECRET', 'demo-client-secret'),
            'tenant_id': os.getenv('POWERBI_TENANT_ID', 'demo-tenant-id')
        }
    
    def validate_request(self, request_data):
        """Validate incoming requests"""
        if not request_data:
            return False, "No data provided"
        
        if not isinstance(request_data, dict):
            return False, "Invalid data format"
        
        return True, "Valid request"
    
    def log_security_event(self, event_type, message):
        """Log security events"""
        timestamp = datetime.now().isoformat()
        log_entry = {
            'timestamp': timestamp,
            'event_type': event_type,
            'message': message
        }
        logging.info(f"Security Event: {json.dumps(log_entry)}")
        
        # Write to security log file
        try:
            with open('/app/output/security.log', 'a') as f:
                f.write(f"{json.dumps(log_entry)}\n")
        except:
            pass  # Fail silently if can't write to file
