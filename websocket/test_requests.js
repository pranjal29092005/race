const PortfolioWebSocketClient = require('./client');
const axios = require('axios');

// Test configurations
const TEST_SCENARIOS = [
    {
        name: 'Quick Test - Stocks Only',
        portfolioTypes: ['stocks'],
        expectedDuration: '5-8 seconds'
    },
    {
        name: 'Medium Test - Stocks & Bonds',
        portfolioTypes: ['stocks', 'bonds'],
        expectedDuration: '8-12 seconds'
    },
    {
        name: 'Full Portfolio Analysis',
        portfolioTypes: ['stocks', 'bonds', 'crypto', 'real_estate'],
        expectedDuration: '15-20 seconds'
    }
];

class PortfolioTester {
    constructor(baseUrl = 'http://localhost:8000') {
        this.baseUrl = baseUrl;
        this.testResults = [];
    }

    // Check if server is running
    async checkServerHealth() {
        try {
            const response = await axios.get(this.baseUrl);
            console.log('✅ Server is running and accessible');
            return true;
        } catch (error) {
            console.error('❌ Server is not accessible:', error.message);
            console.log('💡 Make sure to start the FastAPI server first:');
            console.log('   cd websocket && python main.py');
            return false;
        }
    }

    // Run a single test scenario
    async runTestScenario(scenario, testIndex) {
        console.log(`\n🧪 Test ${testIndex + 1}: ${scenario.name}`);
        console.log(`📋 Portfolio Types: ${scenario.portfolioTypes.join(', ')}`);
        console.log(`⏱️  Expected Duration: ${scenario.expectedDuration}`);
        console.log('─'.repeat(60));

        const startTime = Date.now();
        const client = new PortfolioWebSocketClient(this.baseUrl);
        
        return new Promise((resolve) => {
            let taskId = null;
            let progressUpdates = [];
            let finalResult = null;

            // Override message handler to collect data
            const originalHandler = client.handleWebSocketMessage.bind(client);
            client.handleWebSocketMessage = (message) => {
                originalHandler(message);
                
                if (message.task_id) {
                    progressUpdates.push({
                        timestamp: Date.now(),
                        progress: message.progress_percentage,
                        status: message.status,
                        step: message.current_step
                    });
                    
                    if (message.status === 'completed') {
                        finalResult = message.data;
                        const duration = Date.now() - startTime;
                        
                        const testResult = {
                            scenario: scenario.name,
                            duration: `${(duration / 1000).toFixed(1)}s`,
                            progressUpdates: progressUpdates.length,
                            finalValue: finalResult.total_portfolio_value,
                            success: true
                        };
                        
                        this.testResults.push(testResult);
                        console.log(`✅ Test completed in ${testResult.duration}`);
                        console.log(`💰 Final Portfolio Value: $${finalResult.total_portfolio_value.toLocaleString()}`);
                        
                        client.disconnect();
                        resolve(testResult);
                    } else if (message.status === 'failed') {
                        const testResult = {
                            scenario: scenario.name,
                            duration: `${((Date.now() - startTime) / 1000).toFixed(1)}s`,
                            error: message.current_step,
                            success: false
                        };
                        
                        this.testResults.push(testResult);
                        console.log(`❌ Test failed: ${message.current_step}`);
                        
                        client.disconnect();
                        resolve(testResult);
                    }
                }
            };

            // Start the test
            client.connectWebSocket();
            
            setTimeout(async () => {
                try {
                    taskId = await client.startPortfolioAnalysis(scenario.portfolioTypes);
                } catch (error) {
                    const testResult = {
                        scenario: scenario.name,
                        error: error.message,
                        success: false
                    };
                    this.testResults.push(testResult);
                    client.disconnect();
                    resolve(testResult);
                }
            }, 1000);
            
            // Timeout after 60 seconds
            setTimeout(() => {
                if (progressUpdates.length === 0 || progressUpdates[progressUpdates.length - 1].status === 'processing') {
                    const testResult = {
                        scenario: scenario.name,
                        error: 'Test timeout',
                        success: false
                    };
                    this.testResults.push(testResult);
                    console.log(`⏰ Test timed out`);
                    client.disconnect();
                    resolve(testResult);
                }
            }, 60000);
        });
    }

    // Run all test scenarios
    async runAllTests() {
        console.log('🚀 Starting Portfolio WebSocket API Tests');
        console.log('═'.repeat(60));
        
        // Check server health first
        const serverOk = await this.checkServerHealth();
        if (!serverOk) {
            return;
        }

        // Run each test scenario
        for (let i = 0; i < TEST_SCENARIOS.length; i++) {
            await this.runTestScenario(TEST_SCENARIOS[i], i);
            
            // Wait between tests
            if (i < TEST_SCENARIOS.length - 1) {
                console.log('\n⏳ Waiting 3 seconds before next test...');
                await new Promise(resolve => setTimeout(resolve, 3000));
            }
        }

        // Print summary
        this.printTestSummary();
    }

    // Print test summary
    printTestSummary() {
        console.log('\n📊 TEST SUMMARY');
        console.log('═'.repeat(60));
        
        const successful = this.testResults.filter(r => r.success);
        const failed = this.testResults.filter(r => !r.success);
        
        console.log(`✅ Successful Tests: ${successful.length}`);
        console.log(`❌ Failed Tests: ${failed.length}`);
        console.log(`📈 Success Rate: ${((successful.length / this.testResults.length) * 100).toFixed(1)}%`);
        
        if (successful.length > 0) {
            console.log('\n🎉 Successful Tests:');
            successful.forEach(result => {
                console.log(`   • ${result.scenario} - ${result.duration} - $${result.finalValue?.toLocaleString()}`);
            });
        }
        
        if (failed.length > 0) {
            console.log('\n❌ Failed Tests:');
            failed.forEach(result => {
                console.log(`   • ${result.scenario} - Error: ${result.error}`);
            });
        }
        
        console.log('\n🔍 How to test with Postman:');
        console.log('1. Start Portfolio Analysis:');
        console.log('   POST http://localhost:8000/api/portfolio/analyze');
        console.log('   Body: {"user_id": "test_user", "portfolio_types": ["stocks", "bonds"]}');
        console.log('');
        console.log('2. Check Status (use task_id from step 1):');
        console.log('   GET http://localhost:8000/api/portfolio/status/{task_id}');
        console.log('');
        console.log('3. Get Final Result:');
        console.log('   GET http://localhost:8000/api/portfolio/result/{task_id}');
        console.log('');
        console.log('4. WebSocket Connection:');
        console.log('   ws://localhost:8000/ws/{user_id}');
        console.log('   Send: {"type": "ping"} to test connection');
    }

    // Test REST API endpoints independently
    async testRestEndpoints() {
        console.log('\n🔧 Testing REST API Endpoints');
        console.log('─'.repeat(40));
        
        try {
            // Test analysis start
            console.log('1. Testing analysis start...');
            const startResponse = await axios.post(`${this.baseUrl}/api/portfolio/analyze`, {
                user_id: 'rest_test_user',
                portfolio_types: ['stocks'],
                calculation_type: 'full_analysis'
            });
            
            const taskId = startResponse.data.task_id;
            console.log(`✅ Analysis started - Task ID: ${taskId}`);
            
            // Wait a bit
            await new Promise(resolve => setTimeout(resolve, 2000));
            
            // Test status check
            console.log('2. Testing status check...');
            const statusResponse = await axios.get(`${this.baseUrl}/api/portfolio/status/${taskId}`);
            console.log(`✅ Status retrieved - Progress: ${statusResponse.data.progress_percentage.toFixed(1)}%`);
            
            // Wait for completion (poll until done)
            let completed = false;
            let attempts = 0;
            while (!completed && attempts < 20) {
                await new Promise(resolve => setTimeout(resolve, 1000));
                const status = await axios.get(`${this.baseUrl}/api/portfolio/status/${taskId}`);
                
                if (status.data.status === 'completed') {
                    completed = true;
                    console.log('3. Testing result retrieval...');
                    const resultResponse = await axios.get(`${this.baseUrl}/api/portfolio/result/${taskId}`);
                    console.log(`✅ Result retrieved - Portfolio Value: $${resultResponse.data.total_portfolio_value.toLocaleString()}`);
                } else if (status.data.status === 'failed') {
                    console.log(`❌ Analysis failed: ${status.data.current_step}`);
                    break;
                }
                attempts++;
            }
            
            if (!completed && attempts >= 20) {
                console.log(`⏰ REST API test timed out`);
            }
            
        } catch (error) {
            console.error('❌ REST API test failed:', error.response?.data || error.message);
        }
    }
}

// Main execution
async function main() {
    const tester = new PortfolioTester();
    
    // Run WebSocket tests
    await tester.runAllTests();
    
    // Run REST API tests
    await tester.testRestEndpoints();
    
    console.log('\n🏁 All tests completed!');
    process.exit(0);
}

// Run if this file is executed directly
if (require.main === module) {
    main().catch(error => {
        console.error('Test runner failed:', error);
        process.exit(1);
    });
}

module.exports = PortfolioTester;
