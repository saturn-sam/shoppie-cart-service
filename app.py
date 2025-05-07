
import os
import json
import threading
import time
import pika
from datetime import datetime
from flask import Flask, request, jsonify
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate
from flask_cors import CORS
import jwt
from werkzeug.exceptions import BadRequest, Unauthorized, NotFound

app = Flask(__name__)
CORS(app)

# Database configuration
# app.config['SQLALCHEMY_DATABASE_URI'] = os.environ.get('DATABASE_URL', 'postgresql://postgres:postgres@localhost:5435/cart_db')

uri = os.environ.get('DATABASE_URL', 'postgresql://postgres:postgres@localhost:5439/cart_db')
if uri.startswith('postgres://'):
    uri = uri.replace('postgres://', 'postgresql://', 1)

app.config['SQLALCHEMY_DATABASE_URI'] = uri
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)
migrate = Migrate(app, db)

# RabbitMQ configuration
RABBITMQ_URL = os.environ.get('MESSAGE_QUEUE_URL', 'amqp://guest:guest@localhost:5672')
EXCHANGE_NAME = 'cart_events'

# JWT configuration
JWT_SECRET_KEY = os.environ.get('JWT_SECRET_KEY', 'your-secret-key') # Same as used to encode the token
ALGORITHM = 'HS256' 


import logging
import sys
import json

class JsonFormatter(logging.Formatter):
    def format(self, record):
        return json.dumps({
            "level": record.levelname,
            "message": record.getMessage(),
            "logger": record.name,
            "time": self.formatTime(record, self.datefmt),
        })

handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(JsonFormatter())
handler.setLevel(logging.INFO)

app.logger.handlers = [handler]
app.logger.setLevel(logging.INFO)

file_handler = logging.FileHandler('/var/log/cart.log')
file_handler.setFormatter(JsonFormatter())
file_handler.setLevel(logging.INFO)
app.logger.addHandler(file_handler)

# Models
class CartItem(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, nullable=False)
    product_id = db.Column(db.Integer, nullable=False)
    quantity = db.Column(db.Integer, nullable=False, default=1)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def to_dict(self):
        return {
            'id': self.id,
            'userId': self.user_id,
            'productId': self.product_id,
            'quantity': self.quantity,
            'createdAt': self.created_at.isoformat(),
            'updatedAt': self.updated_at.isoformat()
        }

# Message consumer
def start_consumer():
    try:
        connection = pika.BlockingConnection(pika.URLParameters(RABBITMQ_URL))
        channel = connection.channel()
        app.logger.info('Connected to RabbitMQ')
        
        # Declare both exchanges
        channel.exchange_declare(exchange='product_events', exchange_type='topic', durable=True)

        # Create a queue for this service
        result = channel.queue_declare(queue='cart_service_queue', durable=True)
        queue_name = result.method.queue

        # Bind to purchase events (new requirement)
        channel.queue_bind(exchange='product_events', queue=queue_name, routing_key='purchase.created')
        
        def callback(ch, method, properties, body):
            try:
                with app.app_context():
                    data = json.loads(body)
                    routing_key = method.routing_key
                    
                    if routing_key == 'purchase.created':
                        items = data.get('data', [])
                        for item in items:  # data is a list of items
                            product_id = item.get('productId')
                            user_id = item.get('userId')

                            if product_id and user_id:
                                cart_item = CartItem.query.filter_by(product_id=product_id, user_id=user_id).first()
                                if cart_item:
                                    db.session.delete(cart_item)
                                    db.session.commit()
                                    app.logger.info(f"Deleted cart item for product {product_id} and user {user_id}")
                                else:
                                    app.logger.warning(f"Cart item not found for product {product_id} and user {user_id}")

                ch.basic_ack(delivery_tag=method.delivery_tag)

            except Exception as e:
                app.logger.error(f"Error processing message: {str(e)}")
                ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(queue=queue_name, on_message_callback=callback)
        
        app.logger.info('Started consuming messages from queue')
        channel.start_consuming()
        
    except Exception as e:
        app.logger.error(f"RabbitMQ consumer error: {str(e)}")
        # Retry after delay
        time.sleep(5)
        start_consumer()




def get_user_from_token():
    auth_header = request.headers.get('Authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        return None

    token = auth_header.split(' ')[1]

    try:
        # Decode the JWT
        payload = jwt.decode(token, JWT_SECRET_KEY, algorithms=[ALGORITHM])
        user_id = payload.get('user_id')
        if not user_id:
            return None
        
        # (Optional) Fetch user from DB
        # user = UserModel.query.get(user_id)
        # return user

        return {'id': user_id}

    except jwt.ExpiredSignatureError:
        app.logger.warning("Token has expired")
        return None
    except jwt.InvalidTokenError:
        app.logger.warning("Invalid token")
        return None

def token_required(f):
    def decorator(*args, **kwargs):
        user = get_user_from_token()
        if not user:
            app.logger.warning("Unauthorized access attempt")
            raise Unauthorized('Authentication required')
        request.user = user
        return f(*args, **kwargs)
    decorator.__name__ = f.__name__
    return decorator

# Routes
@app.route('/cart-api/cart/health', methods=['GET'])
def health_check():
    app.logger.info("Health check endpoint called")
    return jsonify({'status': 'healthy', 'service': 'cart-service'}), 200

@app.route('/cart-api/cart', methods=['GET'])
@token_required
def get_cart():
    cart_items = CartItem.query.filter_by(user_id=request.user['id']).all()
    app.logger.info(f"Retrieved {len(cart_items)} items from cart for user {request.user['id']}")
    return jsonify([item.to_dict() for item in cart_items])

@app.route('/cart-api/cart', methods=['POST'])
@token_required
def add_to_cart():
    data = request.get_json()
    product_id = data.get('productId')
    quantity = data.get('quantity', 1)

    if not product_id:
        app.logger.warning("Product ID is required")
        raise BadRequest('Product ID is required')

    existing_item = CartItem.query.filter_by(
        user_id=request.user['id'],
        product_id=product_id
    ).first()

    if existing_item:
        existing_item.quantity += quantity
        db.session.commit()
        app.logger.info(f"Updated quantity for product {product_id} in cart for user {request.user['id']}")
        return jsonify(existing_item.to_dict())

    cart_item = CartItem(
        user_id=request.user['id'],
        product_id=product_id,
        quantity=quantity
    )
    db.session.add(cart_item)
    db.session.commit()
    app.logger.info(f"Added product {product_id} to cart for user {request.user['id']}")
    return jsonify(cart_item.to_dict()), 201

@app.route('/cart-api/cart/<int:item_id>', methods=['PUT'])
@token_required
def update_cart_item(item_id):
    item = CartItem.query.filter_by(id=item_id, user_id=request.user['id']).first_or_404()
    data = request.get_json()
    
    if 'quantity' in data:
        item.quantity = data['quantity']
        if item.quantity <= 0:
            app.logger.info(f"Removing item {item_id} from cart for user {request.user['id']}")
            db.session.delete(item)
        app.logger.info(f"Updated quantity for item {item_id} in cart for user {request.user['id']}")
        db.session.commit()
    
    return jsonify(item.to_dict())

@app.route('/cart-api/cart/<int:item_id>', methods=['DELETE'])
@token_required
def remove_from_cart(item_id):
    item = CartItem.query.filter_by(id=item_id, user_id=request.user['id']).first_or_404()
    db.session.delete(item)
    app.logger.info(f"Removed item {item_id} from cart for user {request.user['id']}")
    db.session.commit()
    return '', 204

# Error handlers
@app.errorhandler(BadRequest)
def handle_bad_request(e):
    app.logger.error(f"Bad request: {str(e)}")
    return jsonify({'error': str(e)}), 400

@app.errorhandler(Unauthorized)
def handle_unauthorized(e):
    app.logger.error(f"Unauthorized access: {str(e)}")
    return jsonify({'error': str(e)}), 401

@app.errorhandler(NotFound)
def handle_not_found(e):
    app.logger.error(f"Not found: {str(e)}")
    return jsonify({'error': str(e)}), 404

# Create tables
# with app.app_context():
#     db.create_all()
print("Starting consumer thread...")
threading.Thread(target=start_consumer, daemon=True).start()

if __name__ == '__main__':

    # print("Starting consumer thread...")
    # consumer_thread = threading.Thread(target=start_consumer)
    # consumer_thread.daemon = True
    # consumer_thread.start()
    app.run(host='0.0.0.0', port=5000)