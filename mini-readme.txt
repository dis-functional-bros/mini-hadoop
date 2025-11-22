mix clean
mix compile
docker-compose down -v
docker-compose build --no-cache
docker-compose up -d
docker ps

docker exec -it master_node iex --remsh master@master --cookie secret
 
MiniHadoop.store_file("A", "test.txt")
MiniHadoop.retrieve_file("A")
MiniHadoop.delete_file("A")


DFBFS.store_file("B", "/shared/test.txt")

docker logs master_node