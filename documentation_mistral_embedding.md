Quickstart
tip

Looking for the AI Studio previously La Plateforme? Head to console.mistral.ai
Get Started
Get Started
Start using Mistral AI API

To get started with Mistral AI, you need to create an account and set up your payment information; once done you can create an API key and start using our API.
Account Setup
Account Setup

    Create a Mistral account or sign in at https://console.mistral.ai.
    Then, navigate to your "Organization" settings at https://admin.mistral.ai.
    To add your payment information and activate payments on your account, find the billing section under Administration.
        You may be later prompted to select a plan; pick between Experiment (free experimental tier) and Scale (pay as you go) plans.
    You can now manage all your Workspaces and Organization via this page.
    Return to https://console.mistral.ai once everything is settled.
    After that, go to the API keys page under your Workspace and create a new API key by clicking "Create new key". Make sure to copy the API key, save it securely, and do not share it with anyone.

Try the API
Try the API
Open in Colab ↗

Mistral AI API provides a seamless way for developers to integrate Mistral's state-of-the-art models into their applications and production workflows with just a few lines of code. Our API is currently available through La Plateforme. You need to activate payments on your account to enable your API keys. After a few moments, you will be able to use our endpoints.

Below, you can see some quickstart code snippets and examples of a few of our endpoints you can visit!

Our Chat Completion endpoint allows you to interact with Mistral AI's models in a conversational manner, pretty much how you would interact with a chatbot.

To learn more about the Chat Completion endpoint, check out our Chat Completions Docs.

import os
from mistralai.client import Mistral

api_key = os.environ["MISTRAL_API_KEY"]
model = "mistral-medium-latest"

client = Mistral(api_key=api_key)

chat_response = client.chat.complete(
    model= model,
    messages = [
        {
            "role": "user",
            "content": "What is the best French cheese?",
        },
    ]
)

Learn More
Learn More

We offer multiple services and models, from transcription to reasoning and sota document AI and OCR systems; For a full description of the models offered on the API, head on to the models page.

Below, you can see some quickstart code snippets and examples of a few of our endpoints you can visit!

The Text Embeddings endpoint allows you to convert text into numerical vectors, which can be used for various NLP tasks such as semantic search, clustering, and classification.

To learn more about the Text Embeddings endpoint, check out our Embeddings Docs.

import os
from mistralai.client import Mistral

api_key = os.environ["MISTRAL_API_KEY"]
model = "mistral-embed"

client = Mistral(api_key=api_key)

embeddings_response = client.embeddings.create(
    model=model,
    inputs=["Embed this sentence.", "As well as this one."]
)

Below, you can see some quickstart code snippets and examples of a few of our endpoints you can visit!

The Text Embeddings endpoint allows you to convert text into numerical vectors, which can be used for various NLP tasks such as semantic search, clustering, and classification.

To learn more about the Text Embeddings endpoint, check out our Embeddings Docs.

curl --location "https://api.mistral.ai/v1/embeddings" \
     --header 'Content-Type: application/json' \
     --header 'Accept: application/json' \
     --header "Authorization: Bearer $MISTRAL_API_KEY" \
     --data '{
    "model": "mistral-embed",
    "input": ["Embed this sentence.", "As well as this one."]
  }'

Text Embeddings

Embeddings are at the core of multiple enterprise use cases, such as retrieval systems, clustering, code analytics, classification, and a variety of search applications. Embedding content, allows you to perform semantic search and diverse NLP tasks for your applications.
Mistral Embed API
Mistral Embed API
Open in Colab ↗
How to Generate Embeddings

To generate text embeddings using Mistral AI's embeddings API, we can make a request to the API endpoint and specify the embedding model mistral-embed, along with providing a list of input texts. The API will then return the corresponding embeddings as numerical vectors, which can be used for further analysis or processing in NLP applications.

import os
from mistralai.client import Mistral

api_key = os.environ["MISTRAL_API_KEY"]
model = "mistral-embed"

client = Mistral(api_key=api_key)

embeddings_batch_response = client.embeddings.create(
    model=model,
    inputs=["Embed this sentence.", "As well as this one."],
)

The output is an embedding object with the embeddings and the token usage information.

Let's take a look at the length of the first embedding:

len(embeddings_batch_response.data[0].embedding)

It returns 1024, which means that our embedding dimension is 1024. The mistral-embed model generates embedding vectors of dimension 1024 for each text string, regardless of the text length. It's worth nothing that while higher dimensional embeddings can better capture text information and improve the performance of NLP tasks, they may require more computational resources for hosting and inference, and may result in increased latency and memory usage for storing and processing these embeddings. This trade-off between performance and computational resources should be considered when designing NLP systems that rely on text embeddings.

Similarity:

In the realm of text embeddings, texts with similar meanings or context tend to be located in closer proximity to each other within this space, as measured by the distance between their vectors. This is due to the fact that the model has learned to group semantically related texts together during the training process.

Let's take a look at a simple example. To simplify working with text embeddings, we can wrap the embedding API in this function:

from sklearn.metrics.pairwise import euclidean_distances

def get_text_embedding(inputs):
    embeddings_batch_response = client.embeddings.create(
        model=model,
        inputs=inputs
    )
    return embeddings_batch_response.data[0].embedding

Suppose we have two sentences: one about cats and the other about books. We want to find how similar each sentence is to the reference sentence "Books are mirrors: You only see in them what you already have inside you". We can see that the distance between the reference sentence embeddings and the book sentence embeddings is smaller than the distance between the reference sentence embeddings and the cat sentence embeddings.

sentences = [
    "A home without a cat — and a well-fed, well-petted and properly revered cat — may be a perfect home, perhaps, but how can it prove title?",
    "I think books are like people, in the sense that they'll turn up in your life when you most need them"
]
embeddings = [get_text_embedding([t]) for t in sentences]

reference_sentence = "Books are mirrors: You only see in them what you already have inside you"
reference_embedding = get_text_embedding([reference_sentence])

for t, e in zip(sentences, embeddings):
    distance = euclidean_distances([e], [reference_embedding])
    print(t, distance)

In our example above, we used the Euclidean distance to measure the distance between embedding vectors (note that since Mistral AI embeddings are norm 1, cosine similarity, dot product or Euclidean distance are all equivalent).

Paraphrase detection:

Another potential use case is paraphrase detection. In this simple example, we have a list of three sentences, and we would like to find out if any of the two sentences are paraphrases of each other. If the distance between two sentence embeddings is small, it suggests that the two sentences are semantically similar and could be potential paraphrases.

The result suggests that the first two sentences are semantically similar and could be potential paraphrases, whereas the third sentence is more different. This is just a super simple example. But this approach can be extended to more complex situations in real-world applications, such as detecting paraphrases in social media posts, news articles, or customer reviews.

import itertools

sentences = [
    "Have a safe happy Memorial Day weekend everyone",
    "To all our friends at Whatsit Productions Films enjoy a safe happy Memorial Day weekend",
    "Where can I find the best cheese?",
]

sentence_embeddings = [get_text_embedding([t]) for t in sentences]

sentence_embeddings_pairs = list(itertools.combinations(sentence_embeddings, 2))
sentence_pairs = list(itertools.combinations(sentences, 2))
for s, e in zip(sentence_pairs, sentence_embeddings_pairs):
    print(s, euclidean_distances([e[0]], [e[1]]))

Batching:

The Mistral AI Embeddings API is designed to process text in batches for improved efficiency and speed. In this example, we will demonstrate this by loading the Symptom2Disease dataset from Kaggle, which contains 1200 rows with two columns: "label" and "text". The "label" column indicates the disease category, while the "text" column describes the symptoms associated with that disease.

We wrote a function get_embeddings_by_chunks that splits data into chunks and then sends each chunk to the Mistral AI Embeddings API to get the embeddings. Then we saved the embeddings as a new column in the dataframe. Note that the API will provide auto-chunking in the future, so that users don't need to manually split the data into chunks before sending it.

import pandas as pd

df = pd.read_csv(
    "https://raw.githubusercontent.com/mistralai/cookbook/main/data/Symptom2Disease.csv",
    index_col=0,
)

def get_embeddings_by_chunks(data, chunk_size):
    chunks = [data[x : x + chunk_size] for x in range(0, len(data), chunk_size)]
    embeddings_response = [
        client.embeddings.create(model=model, inputs=c) for c in chunks
    ]
    return [d.embedding for e in embeddings_response for d in e.data]

df["embeddings"] = get_embeddings_by_chunks(df["text"].tolist(), 50)
df.head()

drawing
t-SNE embeddings visualization
t-SNE embeddings visualization

We mentioned previously that our embeddings have 1024 dimensions, which makes them impossible to visualize directly. Thus, in order to visualize our embeddings, we can use a dimensionality reduction technique such as t-SNE to project our embeddings into a lower-dimensional space that is easier to visualize.

In this example, we transform our embeddings to 2 dimensions and create a 2D scatter plot showing the relationships among embeddings of different diseases.

import seaborn as sns
from sklearn.manifold import TSNE
import numpy as np

tsne = TSNE(n_components=2, random_state=0).fit_transform(np.array(df['embeddings'].to_list()))
ax = sns.scatterplot(x=tsne[:, 0], y=tsne[:, 1], hue=np.array(df['label'].to_list()))
sns.move_legend(ax, 'upper left', bbox_to_anchor=(1, 1))

drawing
Comparison with fastText
Comparison with fastText

We can compare it with fastText, a popular open-source embeddings model. However, when examining the t-SNE embeddings plot, we notice that fastText embeddings fail to create clear separations between data points with matching labels.

import fasttext.util

fasttext.util.download_model('en', if_exists='ignore')  # English
ft = fasttext.load_model('cc.en.300.bin')

df['fasttext_embeddings'] = df['text'].apply(lambda x: ft.get_word_vector(x).tolist())

tsne = TSNE(n_components=2, random_state=0).fit_transform(np.array(df['fasttext_embeddings'].to_list()))
ax = sns.scatterplot(x=tsne[:, 0], y=tsne[:, 1], hue=np.array(df['label'].to_list()))
sns.move_legend(ax, 'upper left', bbox_to_anchor=(1, 1))

drawing

Classification:

Text embeddings can be used as input features in machine learning models, such as classification and clustering. In this example, we use a classification model to predict the disease labels from the embeddings of disease description text from the previous Batching example.

from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression

# Create a train / test split
train_x, test_x, train_y, test_y = train_test_split(
    df["embeddings"], df["label"], test_size=0.2
)

# Normalize features
scaler = StandardScaler()
train_x = scaler.fit_transform(train_x.to_list())
test_x = scaler.transform(test_x.to_list())

# Train a classifier and compute the test accuracy
# For a real problem, C should be properly cross validated and the confusion matrix analyzed
clf = LogisticRegression(random_state=0, C=1.0, max_iter=500).fit(
    train_x, train_y.to_list()
)
# you can also try the sag algorithm:
# clf = LogisticRegression(random_state=0, C=1.0, max_iter=1000, solver='sag').fit(train_x, train_y)

print(f"Precision: {100*np.mean(clf.predict(test_x) == test_y.to_list()):.2f}%")

After we trained the classifier with our embeddings data, we can try classify other text:

# Classify a single example
text = "I've been experiencing frequent headaches and vision problems."
clf.predict([get_text_embedding([text])])

Comparison with fastText embeddings
Comparison with fastText embeddings

Additionally, let's take a look at the performance using fastText embeddings in this classification task. It appears that the classification model achieves better performance with Mistral AI Embeddings model as compared to using fastText embeddings.

# Create a train / test split
train_x, test_x, train_y, test_y = train_test_split(
    df["fasttext_embeddings"], df["label"], test_size=0.2
)

# Normalize features

scaler = StandardScaler()
train_x = scaler.fit_transform(train_x.to_list())
test_x = scaler.transform(test_x.to_list())

# Train a classifier and compute the test accuracy

# For a real problem, C should be properly cross validated and the confusion matrix analyzed

clf = LogisticRegression(random_state=0, C=1.0, max_iter=500).fit(
train_x, train_y.to_list()
)

# you can also try the sag algorithm:

# clf = LogisticRegression(random_state=0, C=1.0, max_iter=1000, solver='sag').fit(train_x, train_y)

print(f"Precision: {100\*np.mean(clf.predict(test_x) == test_y.to_list()):.2f}%")

Our method using Mistral AI Embeddings model achieves a precision of 98.75%, while the fastText embeddings method achieves a precision of 86.25%.

Clustering:

For this section, we will leverage the previous Batching and Classification examples with the same disease dataset.

What if we don't have disease labels? One approach to gain insights from the data is through clustering. Clustering is an unsupervised machine learning technique that groups similar data points together based on their similarity with respect to certain features. In the context of text embeddings, we can use the distance between each embedding as a measure of similarity, and group together data points with embeddings that are close to each other in the high-dimensional space.

Since we already know there are 24 clusters, let's use the K-means clustering with 24 clusters. Then we can inspect a few examples and verify whether the examples in a single cluster are similar to one another. For example, take a look at the first three rows of cluster 23. We can see that they look very similar in terms of symptoms.

from sklearn.cluster import KMeans

model = KMeans(n_clusters=24, max_iter=1000)
model.fit(df['embeddings'].to_list())
df["cluster"] = model.labels_
print(*df[df.cluster==23].text.head(3), sep='\n\n')

Retrieval:

Our embedding model excels in retrieval tasks, as it is trained with retrieval in mind. Embeddings are also incredibly helpful in implementing retrieval-augmented generation (RAG) systems, which use retrieved relevant information from a knowledge base to generate responses. At a high-level, we embed a knowledge base, whether it is a local directory, text files, or internal wikis, into text embeddings and store them in a vector database. Then, based on the user's query, we retrieve the most similar embeddings, which represent the relevant information from the knowledge base. Finally, we feed these relevant embeddings to a large language model to generate a response that is tailored to the user's query and context. If you are interested in learning more about how RAG systems work and how to implement a basic RAG, check out our guide on this topic.
