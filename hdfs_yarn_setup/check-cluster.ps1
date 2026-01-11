echo "=========================================="
echo "Hadoop Cluster Health Check"
echo "=========================================="

echo ""
echo "1. Container Status:"
echo "-------------------------------------------"
docker-compose ps

echo ""
echo "2. HDFS Status:"
echo "-------------------------------------------"
docker exec namenode hdfs dfsadmin -report

echo ""
echo "3. YARN Nodes:"
echo "-------------------------------------------"
docker exec resourcemanager yarn node -list

echo ""
echo "4. YARN Applications:"
echo "-------------------------------------------"
docker exec resourcemanager yarn application -list

echo ""
echo "5. YARN Cluster Info:"
echo "-------------------------------------------"
docker exec resourcemanager yarn cluster -list

echo ""
echo "=========================================="
echo "Health Check Complete!"
echo ""
echo "View detailed info at:"
echo "  HDFS: http://localhost:9870"
echo "  YARN: http://localhost:8088"
echo "=========================================="