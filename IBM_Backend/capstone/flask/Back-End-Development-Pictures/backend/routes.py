from . import app
import os
import json
from flask import jsonify, request, make_response, abort, url_for  # noqa; F401

SITE_ROOT = os.path.realpath(os.path.dirname(__file__))
json_url = os.path.join(SITE_ROOT, "data", "pictures.json")
data: list = json.load(open(json_url))

######################################################################
# RETURN HEALTH OF THE APP
######################################################################


@app.route("/health")
def health():
    return jsonify(dict(status="OK")), 200

######################################################################
# COUNT THE NUMBER OF PICTURES
######################################################################


@app.route("/count")
def count():
    """return length of data"""
    if data:
        return jsonify(length=len(data)), 200

    return {"message": "Internal server error"}, 500


######################################################################
# GET ALL PICTURES
######################################################################
@app.route("/picture", methods=["GET"])
def get_pictures():   
    return data, 200

######################################################################
# GET A PICTURE
######################################################################


@app.route("/picture/<int:id>", methods=["GET"])
def get_picture_by_id(id):
    for picture in data:
        if picture['id'] == id:
            return picture
    
    return {"Message": "picture not found"}, 404



######################################################################
# CREATE A PICTURE
######################################################################
@app.route("/picture", methods=["POST"])
def create_picture():
    picture = request.json

    if not picture:
        return {"Message": "invalid input parameter"}, 422

    if picture in data:
        return {"Message": f"picture with id {picture['id']} already present"}, 302
    else:
        try:
            data.append(picture)
        except NameError:
            return {"Message": "data cannot be added"}, 405

    return picture, 201

######################################################################
# UPDATE A PICTURE
######################################################################


@app.route("/picture/<int:id>", methods=["PUT"])
def update_picture(id):
    newpicture = request.json
    if not newpicture:
        return {"Message": "invalid input parameter"}, 422

    for picture in data:
        if picture['id'] == id:
            picture.update(newpicture)
            return picture, 201
    else:
        return {"Message": "picture not found"}, 404


######################################################################
# DELETE A PICTURE
######################################################################
@app.route("/picture/<int:id>", methods=["DELETE"])
def delete_picture(id):
    for picture in data:
        if picture['id'] == id:
            data.remove(picture)
            return data, 204
        else:
            return {"Message": "picture not found"}, 404
