import datetime
import time
import hashlib
from openai import OpenAI
import numpy as np

def _print(txt="HERE", last_time=time.time()):
    this_time = time.time()
    print("{}\t{}\t{}".format(datetime.datetime.now(),
                              this_time-last_time,
                              txt)
         )
    return time.time()


def generate_unique_id(string):
    return hashlib.md5(string.encode()).hexdigest()


# additions to add vllm served models.
def get_embedding_model(model_name):
    """
    Factory function to create and return the specified embedding model.
    """
    if model_name.startswith('Salesforce'):
        client = get_client()
        models = client.models.list()
        model = models.data[0].id
        print(f'using embedding model: {model}')
        return model
    else:
        # For SentenceTransformer models
        model = SentenceTransformer(model_name)
        print(f'using embedding model: {model}')
        return model

def encode_text(model, text):
    """
    Encode text using the provided model.
    """
    if True: # isinstance(model, LLM):
        # VLLM encoding logic
        client = get_client()
        responses = client.embeddings.create(
            input=[text],
            model=model,
            # encoding_format="float",
        )
        # convert responses into a np.ndarray
        encoded_texts = []
        for data in responses.data:
            # print(data.embedding)  # list of float of len 4096
            encoded_texts.append(np.array(data.embedding, dtype=np.float32))
        return encoded_texts[0]  # TODO standardize this]
    else:
        # SentenceTransformer encoding
        return model.encode(text)

def get_client():
    openai_api_key = "EMPTY"
    openai_api_base = "http://localhost:8000/v1"
    client = OpenAI(
    # defaults to os.environ.get("OPENAI_API_KEY")
        api_key=openai_api_key,
        base_url=openai_api_base,
    )
    return client

def get_model(client):
    models = client.models.list()
    return models.data[0].id


