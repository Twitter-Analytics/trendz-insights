
from flask import Flask, request, jsonify
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import logging

app = Flask(__name__)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initializing NLTK sentiment analyzer
sid = SentimentIntensityAnalyzer()

# Route for predicting sentiment
@app.route('/predict_sentiment', methods=['POST'])
def predict_sentiment():
    try:
        # Getting text data from request
        text = request.json['text']

        # Validate input data
        if not text:
            raise ValueError("Text data is missing")

        # Analyzing sentiment
        sentiment_scores = sid.polarity_scores(text)

        # Returning the sentiment score
        return jsonify(sentiment_scores['compound'])
    except Exception as e:
        logger.error(f"Error predicting sentiment: {str(e)}")
        return jsonify({'error': 'An error occurred while predicting sentiment'}), 500

if __name__ == '__main__':
    app.run(debug=True)