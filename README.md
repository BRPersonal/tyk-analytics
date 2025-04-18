# tyk-analytics
Extract analytics data from redis database

start tyk gateway docker container
$ cd ~/poc/tyk-gateway-docker/
$ docker-compose start

run
python analytics.py

Open postman Select Flask-Tyk-App-Local collection
hit 
GetPosts
GetComments

Once you are done stop the tyk gateway
$ docker-compose stop
