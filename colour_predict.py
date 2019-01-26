import sys
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('colour prediction').getOrCreate()
spark.sparkContext.setLogLevel('WARN')
assert spark.version >= '2.3'  # make sure we have Spark 2.3+
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler, SQLTransformer
from pyspark.ml.classification import MultilayerPerceptronClassifier,  RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from colour_tools import colour_schema, rgb2lab_query, plot_predictions


def main(inputs):
    data = spark.read.csv(inputs, schema=colour_schema)
    rgb_to_lab_query = rgb2lab_query(passthrough_columns=['word'])
    train, validation = data.randomSplit([0.75, 0.25])
    train = train.cache()
    validation = validation.cache()

    # TODO: create a pipeline to predict RGB colours -> word
    rgb_assembler = VectorAssembler(inputCols=["R", "G", "B"], outputCol="features")
    lab_assembler = VectorAssembler(inputCols=["labL", "labA", "labB"], outputCol="features")
    word_indexer = StringIndexer(inputCol="word", outputCol="label", handleInvalid="error")

    classifier_mlp = MultilayerPerceptronClassifier(layers=[3, 30, 11])  
    sqlTrans = SQLTransformer(statement=rgb_to_lab_query)

    pipelines = [
        ('RGB',Pipeline(stages=[rgb_assembler,word_indexer,classifier_mlp])),
        ('LAB', Pipeline(stages=[sqlTrans,lab_assembler,word_indexer,classifier_mlp]))
                ]

    # TODO: create an evaluator and score the validation data
    evaluator = MulticlassClassificationEvaluator(predictionCol="prediction")
    score = dict()
    for label,pipeline in pipelines:
        model = pipeline.fit(train)
        predictions = model.transform(validation)
        score[label] = evaluator.evaluate(predictions)
        plot_predictions(model, label, labelCol='word')
    # TODO: create a pipeline to predict RGB colours -> word; train and evaluate.
    return score

if __name__ == '__main__':
    inputs = sys.argv[1]
    score_dict = main(inputs)
    for key, value in score_dict.items():
        print('Validation score for '+ key + ' model:', value)


