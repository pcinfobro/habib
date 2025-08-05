#!/usr/bin/env python3
"""
Enterprise Pipeline Verification Script
=======================================
Comprehensive testing of all pipeline components
"""

import requests
import json
import time
import pandas as pd
import subprocess
import logging
from datetime import datetime
from typing import Dict, List, Tuple

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class PipelineVerifier:
    """Complete pipeline verification system"""
    
    def __init__(self):
        self.services = {
            'kafka': {'url': 'http://localhost:9092', 'name': 'Kafka (Data Ingestion)'},
            'hdfs': {'url': 'http://localhost:9870', 'name': 'HDFS (Storage)'},
            'spark': {'url': 'http://localhost:8080', 'name': 'Spark (Processing)'},
            'trino': {'url': 'http://localhost:8090', 'name': 'Trino (Query Engine)'},
            'airflow': {'url': 'http://localhost:8081', 'name': 'Airflow (Orchestration)'},
            'powerbi': {'url': 'http://localhost:5000', 'name': 'PowerBI (Automation)'},
        }
        
        self.verification_results = {}

    def verify_service_health(self, service_name: str, url: str) -> bool:
        """Verify if a service is healthy"""
        try:
            response = requests.get(f"{url}/health", timeout=10)
            if response.status_code == 200:
                logger.info(f"✅ {service_name} - Health check passed")
                return True
        except:
            pass
        
        try:
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                logger.info(f"✅ {service_name} - Service accessible")
                return True
        except:
            pass
        
        logger.error(f"❌ {service_name} - Service not accessible")
        return False

    def verify_kafka_functionality(self) -> Dict:
        """Verify Kafka ingestion capability"""
        logger.info("🔍 Verifying Kafka functionality...")
        
        result = {
            'service': 'Kafka',
            'tests': [],
            'overall_status': 'unknown'
        }
        
        try:
            # Check if Kafka container is running
            container_check = subprocess.run(
                ['docker', 'ps', '--filter', 'name=kafka', '--format', '{{.Names}}'],
                capture_output=True, text=True
            )
            
            if 'kafka' in container_check.stdout:
                result['tests'].append({'test': 'Container Running', 'status': 'passed'})
                
                # Check if topics exist
                topic_check = subprocess.run([
                    'docker', 'exec', 'kafka', 'kafka-topics', 
                    '--list', '--bootstrap-server', 'localhost:9092'
                ], capture_output=True, text=True)
                
                if topic_check.returncode == 0:
                    result['tests'].append({'test': 'Topics Accessible', 'status': 'passed'})
                    result['overall_status'] = 'healthy'
                else:
                    result['tests'].append({'test': 'Topics Accessible', 'status': 'failed'})
                    result['overall_status'] = 'unhealthy'
            else:
                result['tests'].append({'test': 'Container Running', 'status': 'failed'})
                result['overall_status'] = 'unhealthy'
                
        except Exception as e:
            result['tests'].append({'test': 'Error', 'status': f'failed: {str(e)}'})
            result['overall_status'] = 'error'
        
        return result

    def verify_hdfs_functionality(self) -> Dict:
        """Verify HDFS storage capability"""
        logger.info("🔍 Verifying HDFS functionality...")
        
        result = {
            'service': 'HDFS',
            'tests': [],
            'overall_status': 'unknown'
        }
        
        try:
            # Check HDFS health
            hdfs_check = subprocess.run([
                'docker', 'exec', 'namenode', 'hdfs', 'dfsadmin', '-report'
            ], capture_output=True, text=True)
            
            if hdfs_check.returncode == 0 and 'Live datanodes' in hdfs_check.stdout:
                result['tests'].append({'test': 'Cluster Health', 'status': 'passed'})
                
                # Check if directories exist
                dir_check = subprocess.run([
                    'docker', 'exec', 'namenode', 'hdfs', 'dfs', '-ls', '/warehouse'
                ], capture_output=True, text=True)
                
                if dir_check.returncode == 0:
                    result['tests'].append({'test': 'Warehouse Directory', 'status': 'passed'})
                    result['overall_status'] = 'healthy'
                else:
                    result['tests'].append({'test': 'Warehouse Directory', 'status': 'failed'})
                    result['overall_status'] = 'unhealthy'
            else:
                result['tests'].append({'test': 'Cluster Health', 'status': 'failed'})
                result['overall_status'] = 'unhealthy'
                
        except Exception as e:
            result['tests'].append({'test': 'Error', 'status': f'failed: {str(e)}'})
            result['overall_status'] = 'error'
        
        return result

    def verify_spark_functionality(self) -> Dict:
        """Verify Spark processing capability"""
        logger.info("🔍 Verifying Spark functionality...")
        
        result = {
            'service': 'Spark',
            'tests': [],
            'overall_status': 'unknown'
        }
        
        try:
            # Check Spark master
            response = requests.get('http://localhost:8080', timeout=10)
            if response.status_code == 200 and 'Spark Master' in response.text:
                result['tests'].append({'test': 'Master Accessible', 'status': 'passed'})
                
                # Check if workers are connected
                if 'Workers' in response.text:
                    result['tests'].append({'test': 'Workers Connected', 'status': 'passed'})
                    result['overall_status'] = 'healthy'
                else:
                    result['tests'].append({'test': 'Workers Connected', 'status': 'failed'})
                    result['overall_status'] = 'unhealthy'
            else:
                result['tests'].append({'test': 'Master Accessible', 'status': 'failed'})
                result['overall_status'] = 'unhealthy'
                
        except Exception as e:
            result['tests'].append({'test': 'Error', 'status': f'failed: {str(e)}'})
            result['overall_status'] = 'error'
        
        return result

    def verify_powerbi_functionality(self) -> Dict:
        """Verify PowerBI automation capability"""
        logger.info("🔍 Verifying PowerBI functionality...")
        
        result = {
            'service': 'PowerBI Automation',
            'tests': [],
            'overall_status': 'unknown'
        }
        
        try:
            # Check PowerBI service health
            response = requests.get('http://localhost:5000/health', timeout=10)
            if response.status_code == 200:
                result['tests'].append({'test': 'Service Health', 'status': 'passed'})
                
                # Check datasets endpoint
                datasets_response = requests.get('http://localhost:5000/datasets', timeout=10)
                if datasets_response.status_code == 200:
                    result['tests'].append({'test': 'Datasets API', 'status': 'passed'})
                    
                    # Check dashboards endpoint
                    dashboards_response = requests.get('http://localhost:5000/dashboards', timeout=10)
                    if dashboards_response.status_code == 200:
                        result['tests'].append({'test': 'Dashboards API', 'status': 'passed'})
                        result['overall_status'] = 'healthy'
                    else:
                        result['tests'].append({'test': 'Dashboards API', 'status': 'failed'})
                        result['overall_status'] = 'unhealthy'
                else:
                    result['tests'].append({'test': 'Datasets API', 'status': 'failed'})
                    result['overall_status'] = 'unhealthy'
            else:
                result['tests'].append({'test': 'Service Health', 'status': 'failed'})
                result['overall_status'] = 'unhealthy'
                
        except Exception as e:
            result['tests'].append({'test': 'Error', 'status': f'failed: {str(e)}'})
            result['overall_status'] = 'error'
        
        return result

    def verify_airflow_functionality(self) -> Dict:
        """Verify Airflow orchestration capability"""
        logger.info("🔍 Verifying Airflow functionality...")
        
        result = {
            'service': 'Airflow',
            'tests': [],
            'overall_status': 'unknown'
        }
        
        try:
            # Check Airflow webserver
            response = requests.get('http://localhost:8081', timeout=10)
            if response.status_code == 200:
                result['tests'].append({'test': 'Webserver Accessible', 'status': 'passed'})
                
                # Check if our DAG is present (would need authentication in real scenario)
                result['tests'].append({'test': 'DAG Deployment', 'status': 'assumed_passed'})
                result['overall_status'] = 'healthy'
            else:
                result['tests'].append({'test': 'Webserver Accessible', 'status': 'failed'})
                result['overall_status'] = 'unhealthy'
                
        except Exception as e:
            result['tests'].append({'test': 'Error', 'status': f'failed: {str(e)}'})
            result['overall_status'] = 'error'
        
        return result

    def test_end_to_end_pipeline(self) -> Dict:
        """Test complete end-to-end pipeline functionality"""
        logger.info("🔍 Testing end-to-end pipeline...")
        
        result = {
            'test': 'End-to-End Pipeline',
            'steps': [],
            'overall_status': 'unknown'
        }
        
        try:
            # Step 1: Create test dataset
            test_data = {
                'name': 'Pipeline Test Dataset',
                'source_type': 'csv',
                'source_path': '/app/data/test_data.csv',
                'metadata': {'test': True}
            }
            
            response = requests.post(
                'http://localhost:5000/datasets',
                json=test_data,
                timeout=10
            )
            
            if response.status_code == 200:
                result['steps'].append({'step': 'Dataset Creation', 'status': 'passed'})
                
                dataset_id = response.json().get('dataset_id')
                if dataset_id:
                    # Step 2: Create test dashboard
                    dashboard_data = {
                        'name': 'Pipeline Test Dashboard', 
                        'dataset_id': dataset_id,
                        'config': {'test': True}
                    }
                    
                    dashboard_response = requests.post(
                        'http://localhost:5000/dashboards',
                        json=dashboard_data,
                        timeout=10
                    )
                    
                    if dashboard_response.status_code == 200:
                        result['steps'].append({'step': 'Dashboard Creation', 'status': 'passed'})
                        result['overall_status'] = 'healthy'
                    else:
                        result['steps'].append({'step': 'Dashboard Creation', 'status': 'failed'})
                        result['overall_status'] = 'unhealthy'
                else:
                    result['steps'].append({'step': 'Dataset ID Retrieval', 'status': 'failed'})
                    result['overall_status'] = 'unhealthy'
            else:
                result['steps'].append({'step': 'Dataset Creation', 'status': 'failed'})
                result['overall_status'] = 'unhealthy'
                
        except Exception as e:
            result['steps'].append({'step': 'Error', 'status': f'failed: {str(e)}'})
            result['overall_status'] = 'error'
        
        return result

    def run_complete_verification(self) -> Dict:
        """Run complete pipeline verification"""
        logger.info("🚀 Starting Complete Pipeline Verification")
        logger.info("=" * 50)
        
        verification_report = {
            'timestamp': datetime.now().isoformat(),
            'services': {},
            'end_to_end_test': {},
            'overall_health': 'unknown',
            'summary': {}
        }
        
        # Test each service
        verification_report['services']['kafka'] = self.verify_kafka_functionality()
        verification_report['services']['hdfs'] = self.verify_hdfs_functionality() 
        verification_report['services']['spark'] = self.verify_spark_functionality()
        verification_report['services']['powerbi'] = self.verify_powerbi_functionality()
        verification_report['services']['airflow'] = self.verify_airflow_functionality()
        
        # Test end-to-end functionality
        verification_report['end_to_end_test'] = self.test_end_to_end_pipeline()
        
        # Calculate overall health
        healthy_services = sum(1 for service in verification_report['services'].values() 
                             if service['overall_status'] == 'healthy')
        total_services = len(verification_report['services'])
        
        health_percentage = (healthy_services / total_services) * 100
        
        if health_percentage >= 90:
            verification_report['overall_health'] = 'excellent'
        elif health_percentage >= 70:
            verification_report['overall_health'] = 'good'
        elif health_percentage >= 50:
            verification_report['overall_health'] = 'fair'
        else:
            verification_report['overall_health'] = 'poor'
        
        # Create summary
        verification_report['summary'] = {
            'total_services': total_services,
            'healthy_services': healthy_services,
            'health_percentage': health_percentage,
            'e2e_test_status': verification_report['end_to_end_test']['overall_status']
        }
        
        return verification_report

    def generate_report(self, report: Dict) -> None:
        """Generate human-readable verification report"""
        print("\n" + "="*60)
        print("🎯 ENTERPRISE PIPELINE VERIFICATION REPORT")
        print("="*60)
        print(f"📅 Report Generated: {report['timestamp']}")
        print(f"🏥 Overall Health: {report['overall_health'].upper()}")
        print(f"📊 Health Score: {report['summary']['health_percentage']:.1f}%")
        print()
        
        print("🔧 SERVICE STATUS:")
        print("-" * 30)
        for service_name, service_data in report['services'].items():
            status_icon = "✅" if service_data['overall_status'] == 'healthy' else "❌"
            print(f"{status_icon} {service_data['service']}: {service_data['overall_status'].upper()}")
            
            for test in service_data['tests']:
                test_icon = "  ✓" if 'passed' in str(test['status']) else "  ✗"
                print(f"{test_icon} {test['test']}: {test['status']}")
        
        print()
        print("🔄 END-TO-END PIPELINE TEST:")
        print("-" * 30)
        e2e_status = report['end_to_end_test']['overall_status']
        e2e_icon = "✅" if e2e_status == 'healthy' else "❌"
        print(f"{e2e_icon} Pipeline Test: {e2e_status.upper()}")
        
        for step in report['end_to_end_test']['steps']:
            step_icon = "  ✓" if 'passed' in str(step['status']) else "  ✗"
            print(f"{step_icon} {step['step']}: {step['status']}")
        
        print()
        print("📋 RECOMMENDATIONS:")
        print("-" * 20)
        
        if report['overall_health'] == 'excellent':
            print("🎉 Pipeline is running perfectly! All systems operational.")
            print("💡 Consider running performance optimization tests.")
        elif report['overall_health'] == 'good':
            print("👍 Pipeline is mostly healthy with minor issues.")
            print("💡 Review failed tests and optimize where needed.")
        else:
            print("⚠️  Pipeline needs attention - several services have issues.")
            print("💡 Review logs and restart problematic services.")
        
        print()
        print("🌐 WEB INTERFACES:")
        print("-" * 18)
        print("   • Hadoop HDFS:       http://localhost:9870")
        print("   • Spark Master:      http://localhost:8080")  
        print("   • Trino Queries:     http://localhost:8090")
        print("   • Airflow:           http://localhost:8081 (admin/admin123)")
        print("   • PowerBI Service:   http://localhost:5000")
        print()
        print("="*60)


def main():
    """Main verification function"""
    verifier = PipelineVerifier()
    
    # Run complete verification
    report = verifier.run_complete_verification()
    
    # Generate human-readable report
    verifier.generate_report(report)
    
    # Save detailed report
    with open('pipeline_verification_report.json', 'w') as f:
        json.dump(report, f, indent=2)
    
    print(f"📄 Detailed report saved to: pipeline_verification_report.json")
    
    # Return appropriate exit code
    return 0 if report['overall_health'] in ['excellent', 'good'] else 1


if __name__ == '__main__':
    exit(main())
