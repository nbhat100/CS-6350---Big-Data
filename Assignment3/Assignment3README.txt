Section 1: Structured Streaming with Kafka and ELK
	Please use the following steps to run my program, and visualize the named entities and counts in Kibana
	
	1. Install Kafka (https://www.apache.org/dyn/closer.cgi?path=/kafka/3.6.0/kafka_2.13-3.6.0.tgz) and unzip
	2. Run zookeper:
		a. cd into kafka directory
		b. Run the following command: bin/zookeeper-server-start.sh config/zookeeper.properties
	3. Start Kafka broker:
		a. cd into kafka directory
		b. Run the following command: bin/kafka-server-start.sh config/server.properties
	4. Start the ELK stack
		a. Download Elasticsearch, Logstash, and Kibana from https://www.elastic.co/downloads, and unzip
		b. Start Elasticsearch
			- cd into elasticsearch directory
			- run bin/elasticsearch
			- The first time this is run, it will show the elastic password and Kibana enrollment token. Please make a note of these.
		c. Start Kibana
			- cd into kibana directory
			- run bin/kibana
			- The first time this is run, it will give you a link to the elasticsearch web interface.
				It will first ask for the enrollment token, paste the token from earlier.
				Then it will ask for the username and password. Enter the following:
					username: elastic
					password: <your password given from elasticsearch earlier>
		d. Start Logstash
			- cd into logstash directory
			- create logstash conf file by executing: vim assignment3ELK.conf
			- copy and paste the below configurations. Replace the text in <> with your values. The cacert path
				should be the http_ca.crt file from the elasticsearch certs directory

				input {
				  kafka{
					bootstrap_servers => "localhost:9092"
					topics => "<topic-name>"]
				  }
				}

				filter {
				  json {
					source => "message"
					target => "doc"
				  }
				}

				output {
				  elasticsearch {
					  hosts => ["https://localhost:9200/"]
					  index => "assignment3"
					  user => "elastic"
					  password => "<your-password>"
					  ssl => true
					  cacert => "<your-cert-path>"

				  }
				}
			- Run bin/logstash -f assignment3ELK.conf
	5. Get the comments new posts in the r/news subreddit
		a. cd into the directory with the python source files
		b. Run the following command: python streamRedditData.py <bootstrap-servers> <outputTopic>
	6. Run the python file that will consume the real time data from Reddit:
		a. cd into the directory with the python source files
		b. Run the following command: spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:<your spark version> consumeRedditData.py <bootstrap-servers> <subscribe-type> <inputTopic> <outputTopic>
	7. View the bar graphs
		a. Go to localhost:5601 in your browser, enter the elastic username and password from earlier
		b. Go to Stack Management --> Index Management
		c. Click on the assignment3 index
		d. Click Discover index
		e. Make the following configurations:
			- Make the graph a horizontal bar graph
			- Put the doc.entity.keyword on the vertical axis
			- Pick the top 10 values, go to advanced and remove the "other" category checkmark
			- Put the doc.count attribute on the horizontal axis
			- Pick the last value function
		f. Click save
		g. Change refresh and time interval settings


Section 2: Analyzing Social Networks using GraphX/GraphFrame
	Please use the following steps to run my program.
	1. Go to https://snap.stanford.edu/data/congress-twitter.html, and download and unzip the zip file
	3. Install Graphframes, and run spark with the Graphframes package 
		spark-submit --packages graphframes:graphframes:0.8.3-spark3.5-s_2.12 socialNetworkAnalysis.py <inputDatasetPath> <outputFilePath>
	4. In case the 3rd step does not work, you can run the code on this Colab Link:
		https://colab.research.google.com/drive/1PJ8RzZ0519nEsY7woBVIYFpkT67hADOu?usp=sharing
		Please make sure to upload the congress.edgelist and congress_network_data.json file to the Colab session.
		
		
data governance
checkointing 
etl pipeline
preventing data leakage
data lake vs data warehouse