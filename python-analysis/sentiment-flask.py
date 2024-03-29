from flask import Flask, request, jsonify
from nltk.sentiment.vader import SentimentIntensityAnalyzer

# Initializing Flask application
import nltk
nltk.download('vader_lexicon')

app = Flask(__name__)

# Initializing NLTK sentiment analyzer
sid = SentimentIntensityAnalyzer()

# Route for predicting sentiment
@app.route('/predict_sentiment', methods=['POST'])
def predict_sentiment():
    # Getting text data from request
    text = request.json['text']

    # Analyzing sentiment
    sentiment_scores = sid.polarity_scores(text)

    # Determining sentiment label based on compound score
    if sentiment_scores['compound'] >= 0.05:
        sentiment_label = 'Positive'
    elif sentiment_scores['compound'] <= -0.05:
        sentiment_label = 'Negative'
    else:
        sentiment_label = 'Neutral'

    # Returning the result
    return jsonify({'sentiment': sentiment_label , 'score' : sentiment_scores['compound']})

if __name__ == '__main__':
    app.run(debug=True)