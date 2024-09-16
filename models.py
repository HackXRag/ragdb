import os
import numpy as np
from sentence_transformers import SentenceTransformer
from openai import OpenAI

"""
Module to wrap up access to our models and the model serving environment

We use a "framework name" to point the model at either a local version or the VLLM one.

"""

class EmbeddingModel:

    def __init__(self, model_name, framework_name, endpoint="http://localhost:8000/v1"):
        if framework_name == "SentenceTransformer":
            self.model = SentenceTransformer(model_name)
        elif framework_name == "VLLM":
            open_api_key="EMPTY"
            openai_api_base = endpoint
            client = OpenAI(
                api_key=open_api_key,
                base_url=openai_api_base,
                )
            models = client.models.list()
            if not models.data[0].id == model_name:
                raise ValueError(f"Invalid model_name {model_name}")

            self.model = client
        else:
            # throw an exception
            raise ValueError(f"Invalid framework_name: {framework_name}.")
        
    def encode(self, query_text):
        model = self.model

        if isinstance(model, SentenceTransformer):
            query_embedding = model.encode(query_text)
            query_embedding = np.array([query_embedding]).astype('float32')
            return query_embedding[0][0]
    
        elif isinstance(model, OpenAI):
            client = model
            models = client.models.list()
            model_name = models.data[0].id
            print(f"using model name {model_name} with OpenAI interface")
            responses = client.embeddings.create(
                input=query_text,
                model=model_name,
                )
        #for data in responses.data:
            #print(data.embedding)  # list of float of len 4096
            x = [ data.embedding for data in responses.data  ]
            return np.array(x[0]).astype('float32')
        
        else:
            raise TypeError(f'model is type {type(model)}')

