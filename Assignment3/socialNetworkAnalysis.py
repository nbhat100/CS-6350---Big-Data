import pandas as pd
import sys
import networkx as nx
import json
import warnings
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import *
from pyspark.sql.functions import count, desc
from graphframes import *
from graphframes import GraphFrame

import findspark
findspark.init()

if len(sys.argv) != 3:
    print("""
        Usage: socialNetworkAnalysis.py <inputFolderPath> <outputFileName>
        """)
    sys.exit(-1)

spark = SparkSession.builder.appName("SocialNetwork").getOrCreate()

spark.conf.set("spark.sql.repl.eagerEval.enabled", True)  # Property used to format output tables better\
sc = spark.sparkContext

warnings.filterwarnings('ignore')

curGraph = nx.DiGraph()
inputDatasetPath = sys.argv[1]
outputFilePath = sys.argv[2]
with open(inputDatasetPath + '/congress.edgelist', 'r') as file:
    lines = file.readlines()
    for line in lines:
        if line:
            line = line.split()
            fromNode = int(line[0])
            toNode = int(line[1])
            weightString = line[3]
            curWeight = float(weightString[: len(weightString) - 1])
            curGraph.add_edge(fromNode, toNode, weight=curWeight)


graphDF = nx.to_pandas_edgelist(curGraph)
congressMembers = []
with open(inputDatasetPath + '/congress_network_data.json') as file:
    congressData = json.load(file)
    congressMembers = congressData[0]['usernameList']

nodesDF = pd.DataFrame()
uniqueNodes = list(graphDF['source'].unique())
idToNameMap = {}
for id in uniqueNodes:
    idToNameMap[id] = congressMembers[id]

nodesDF['id'] = uniqueNodes
nodesDF['name'] = list(idToNameMap.values())

graphDF.rename(columns={'source': 'src'}, inplace=True)
graphDF.rename(columns={'target': 'dst'}, inplace=True)

vertices = spark.createDataFrame(nodesDF, ['id', 'name'])
edges = spark.createDataFrame(graphDF, ['src', 'dst', 'edgeWeight'])

curGF = GraphFrame(vertices, edges)
with open(outputFilePath + "/outputData.txt", 'a+') as file:

    # Queries
    # a. Find the top 5 nodes with the highest outdegree and find the
    #    count of the number of outgoing edges in each
    outDegreeAndNames = curGF.outDegrees.join(curGF.vertices, on="id")
    outDegreeAndNames = outDegreeAndNames.select('id', 'name', 'outDegree')
    sortedOutDegreesAndNames = outDegreeAndNames.orderBy(desc("outDegree")).limit(5)
    outDegDF = sortedOutDegreesAndNames.to_pandas_on_spark()
    dfString = outDegDF.to_string()
    file.write(dfString)
    file.write('\n\n')
    sortedOutDegreesAndNames.show()

    # b. Find the top 5 nodes with the highest indegree and find the count of the number of incoming edges
    #    in each
    inDegreeAndNames = curGF.inDegrees.join(curGF.vertices, on="id")
    inDegreeAndNames = inDegreeAndNames.select('id', 'name', 'inDegree')
    sortedInDegreesAndNames = inDegreeAndNames.orderBy(desc("inDegree")).limit(5)
    dfString = sortedInDegreesAndNames.to_pandas_on_spark().to_string()
    file.write(dfString)
    file.write('\n\n')
    sortedInDegreesAndNames.show()

    # c. Calculate PageRank for each of the nodes and output the top 5 nodes with the highest PageRank
    #    values. You are free to define any suitable parameters.
    results = curGF.pageRank(maxIter = 10)
    results = results.vertices.orderBy(desc("pagerank")).limit(5)
    resultDdfString = results.to_pandas_on_spark().to_string()
    file.write(dfString)
    file.write('\n\n')
    results.show()

    # d. Run the connected components algorithm on it and find the top 5 components with the largest
    #    number of nodes.
    sc.setCheckpointDir("/tmp")
    graphConnectedComps = curGF.connectedComponents()
    graphConnectedComps = graphConnectedComps.select('id', 'name', 'component').orderBy("component").limit(5)
    dfString = graphConnectedComps.to_pandas_on_spark().to_string()
    file.write(dfString)
    file.write('\n\n')
    graphConnectedComps.show()

    # e. Run the triangle counts algorithm on each of the vertices and output the top 5 vertices with the
    #    largest triangle count. In case of ties, you can randomly select the top 5 vertices.
    triangleCount = curGF.triangleCount()
    result = triangleCount.select('id', 'name', 'count').orderBy(desc("count")).limit(5)
    dfString = result.to_pandas_on_spark().to_string()
    file.write(dfString)
    result.show()