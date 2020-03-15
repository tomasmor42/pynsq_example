from marshmallow import Schema, fields

class MessageSchema(Schema):
    user = fields.Str()
    origin = fields.Str()
    destination = fields.Str()
