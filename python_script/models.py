import json

from torchvision import models
import torch
from torchvision import transforms
from flask import Flask, Response, request
import categories
from PIL import Image

app = Flask(__name__)
# base_file_path = "/Users/suzy/Desktop/mp4_data/model/test/data/"
base_file_path = "/home/jadonts2/model/test/data/";

class Resnet:
    def __init__(self):
        self.model = None
        self.transform = transforms.Compose([
            transforms.Resize(256),
            transforms.CenterCrop(224),
            transforms.ToTensor(),
            transforms.Normalize(mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225])])
        self.device = torch.device("cpu")

    def train(self):
        response = Response()
        try:
            self.model = models.resnet101(pretrained=True)
            self.model.to(self.device)
            self.model.eval()
            response.status_code = 200
            response.data = json.dumps({"message": "Training Success"})
            return response
        except Exception as e:
            print(e)
            response.status_code = 500
            response.data = json.dumps({"message": "Model training failed"})
            return response

    def test(self, filepaths):
        response = Response()
        if not self.model:
            response.status_code = 500
            response.data = json.dumps({"message": "Model not yet trained. Train first"})
            return response
        try:
            result = []
            for filepath in filepaths:
                filepath = base_file_path + filepath
                try:
                    img = Image.open(filepath)
                    img_t = self.transform(img)
                    batch_t = torch.unsqueeze(img_t, 0)
                    out = self.model(batch_t)
                    _, index = torch.max(out, 1)
                    percentage = torch.nn.functional.softmax(out, dim=1)[0] * 100
                    _, indices = torch.sort(out, descending=True)
                    result.append(str({"fileName": filepath, "label": categories._IMAGENET_CATEGORIES[indices[0][0]],  "confidence_percentage" : str(round(percentage[indices[0][0]].item()))}))
                except Exception as e:
                    result.append("Unformatted data. Could not classify")
            response.status_code = 200
            response.data = json.dumps({"message": "Training Success", "result": result})
            return response
        except Exception as e:
            print(e)
            response.status_code = 500
            response.data = json.dumps({"message": "Model training failed"})
            return response

class InceptionV3:
    def __init__(self):
        self.model = None
        self.transform = transforms.Compose([
            transforms.Resize(299),
            transforms.CenterCrop(299),
            transforms.ToTensor(),
            transforms.Normalize(mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225]),
            ])
        self.device = torch.device("cpu")

    def train(self):
        response = Response()
        try:
            self.model = models.inception_v3(pretrained=True)
            self.model.to(self.device)
            self.model.eval()
            response.status_code = 200
            response.data = json.dumps({"message": "Training Success"})
            return response
        except Exception as e:
            print(e)
            response.status_code = 500
            response.data = json.dumps({"message": "Model training failed"})
            return response

    def test(self, filepaths):
        response = Response()
        if not self.model:
            response.status_code = 500
            response.data = json.dumps({"message": "Model not yet trained. Train first"})
            return response
        try:
            result = []
            for filepath in filepaths:
                filepath = base_file_path + filepath
                try:
                    img = Image.open(filepath)
                    img_t = self.transform(img)
                    batch_t = torch.unsqueeze(img_t, 0)
                    out = self.model(batch_t)
                    _, index = torch.max(out, 1)
                    percentage = torch.nn.functional.softmax(out, dim=1)[0] * 100
                    _, indices = torch.sort(out, descending=True)
                    result.append(str({"fileName": filepath, "label": categories._IMAGENET_CATEGORIES[indices[0][0]],  "confidence_percentage" : str(round(percentage[indices[0][0]].item()))}))
                except Exception as e:
                    result.append("Unformatted data. Could not classify")
            response.status_code = 200
            response.data = json.dumps({"message": "Training Success", "result": result})
            return response
        except Exception as e:
            print(e)
            response.status_code = 500
            response.data = json.dumps({"message": "Model training failed"})
            return response

resnet = Resnet()
inception = InceptionV3()

@app.route('/resnet/train', methods=['GET'])
def train_resnet():
    return resnet.train()

@app.route('/resnet/test', methods=['POST'])
def test_resnet():
    data = request.get_json()
    return resnet.test(data['filepaths'])

@app.route('/inception/train', methods=['GET'])
def train_inception():
    return resnet.train()

@app.route('/inception/test', methods=['POST'])
def test_inception():
    data = request.get_json()
    return resnet.test(data['filepaths'])

app.run(port=8080)


