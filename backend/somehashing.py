import mmap
import os
import struct
import string
import nltk
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer
import re
from flask import Flask, request, jsonify
from flask_socketio import SocketIO
import os
from werkzeug.utils import secure_filename
import json
import signal
import time
import duckdb
from collections import defaultdict
from flask_cors import CORS
import fitz  # PyMuPDF
import traceback
import requests

from algorithms.MMapChainedHashTable import MMapChainedHashTable
from algorithms.MergeSort import MergeSort
from algorithms.Preprocessor import Preprocessor

GEMINI_API_KEY = 'AIzaSyDqPDzL7eu12n1t9wib5hQoM8d5uDdUNg4'  # Replace with your Gemini API key
GEMINI_API_URL = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key={GEMINI_API_KEY}"

nltk.download('punkt')
nltk.download('stopwords')

p = Preprocessor()

def preprocess_text(text):
    tokens = p.preprocess_text(text)

    return tokens

# Insert tokens for a given doc_id, adding doc_id only once per token

def insert_doc_tokens(ht, tokens, doc_id):
    unique_tokens = set(tokens)
    for token in unique_tokens:
        ht.insert(token, [doc_id])

def process_text_string(ht, text, doc_id):
    tokens = preprocess_text(text)
    insert_doc_tokens(ht, tokens, doc_id)

# Example usage:
"""
if __name__ == "__main__":
    ht = MMapChainedHashTable()

    text1 = "Artificial intelligence is the future of technology."
    text2 = "Python is a popular programming language."
    text3 = "AI and Python often go hand in hand."

    process_text_string(ht, text1, doc_id=1)
    process_text_string(ht, text2, doc_id=2)
    process_text_string(ht, text3, doc_id=3)

    print("ai docs:", ht.get("ai"))         # Should show [1, 3]
    print("python docs:", ht.get("python")) # Should show [2, 3]
    print("python docs:", ht.get("programming")) # Should show [2, 3]
    ht.close()
"""

def score_jobs(ht, resume_tokens, top_n=1000):
    scores = defaultdict(int)
    
    for token in set(resume_tokens):  # unique tokens to avoid double counting
        doc_ids = ht.get(token)
        if doc_ids:
            for doc_id in doc_ids:
                scores[doc_id] += 1
    
    m = MergeSort()
    sorted_jobs = m.merge_sort(list(scores.items()))
    top_jobs = sorted_jobs[:top_n]
    
    return top_jobs

def truncate_text(text, max_len=300):
    if len(text) > max_len:
        return text[:max_len].rstrip() + "..."
    return text

def paginate_results(results, page_size=10):
    """Splits (doc_id, score) tuples into pages."""
    pages = {}
    for i in range(0, len(results), page_size):
        page_number = (i // page_size) + 1
        pages[page_number] = results[i:i + page_size]
    print("Pages: ", pages)
    return pages

def get_job_snippet(con, doc_id):
    result = con.execute("""
        SELECT title, description, location, company_name
        FROM linkedin_jobs
        WHERE id = ?
    """, [doc_id]).fetchone()
    if result:
        title, job_desc, location, company = result
        return {
            "doc_id": doc_id,
            "title": title,
            "score": None,  # optionally updated later
            "snippet": truncate_text(job_desc),
            "location" : location or None,
            "company_name" : company or None
        }

def extract_text_from_pdf(pdf_path):
    doc = fitz.open(pdf_path)
    text = ""
    for page in doc:
        text += page.get_text()
    return text

def get_page(con, pages, page_num):
    # Example: send page 1 to client
    page_data = []
    
    for doc_id, score in pages.get(page_num, []):
        job = get_job_snippet(con, doc_id)
        if job:
            job["score"] = score
            page_data.append(job)
    return page_data

app = Flask(__name__)
CORS(app)
socketio = SocketIO(app, cors_allowed_origins="*")

def close_process_by_pid(pid):
    """Kill a process by its PID (works on both Windows and Linux)."""
    try:
        print(f"Closing process with PID {pid}...")
        if os.name == 'nt':  # Windows
            os.kill(int(pid), 9)  # Hard kill on Windows
        else:  # Linux/Mac
            os.kill(int(pid), signal.SIGKILL)  # Proper kill on Unix-based systems
    except (ValueError, ProcessLookupError, PermissionError):
        print(f"Failed to close process {pid}. It may not exist or lack permissions.")

def extract_pid_from_error(error_message):
    """Extract PID from DuckDB error message."""
    match = re.search(r"PID (\d+)", error_message)
    return match.group(1) if match else None

def connect_to_duckdb(db_path):
    """Try connecting to DuckDB and handle locked database errors."""
    attempts = 3  # Number of retries
    for attempt in range(attempts):
        try:
            print(f"Attempt {attempt + 1}: Connecting to DuckDB at {db_path}...")
            conn = duckdb.connect(db_path)
            print("Connected successfully!")
            return conn
        except duckdb.IOException as e:
            error_msg = str(e)
            print("Database lock detected:", error_msg)

            # Extract PID from error message and close it
            pid = extract_pid_from_error(error_msg)
            if pid:
                close_process_by_pid(pid)
                time.sleep(1)  # Wait before retrying
            else:
                print("No PID found in error message.")
                raise  # Raise if the error is not related to PID lock

    print("Failed to connect after multiple attempts.")
    return None  # Return None if unable to connect
import zipfile
import os

zip_file_path = 'linkedin_jobs.zip'
extracted_file_name = 'linkedin_jobs.db'
extraction_path = '.' # You can change this to a specific directory if needed

if not os.path.exists(os.path.join(extraction_path, extracted_file_name)):
    try:
        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            zip_ref.extract(extracted_file_name, extraction_path)
        print(f"Successfully extracted {extracted_file_name} from {zip_file_path}")
    except FileNotFoundError:
        print(f"Error: The file {zip_file_path} was not found.")
    except KeyError:
        print(f"Error: {extracted_file_name} not found inside {zip_file_path}.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
else:
    print(f"{extracted_file_name} already exists at {extraction_path}. No extraction needed.")

con = connect_to_duckdb('linkedin_jobs.db')
ht = MMapChainedHashTable()

if __name__ == "__main__":
    # Connect to your DuckDB database
    #Create some function that will download linkedin index

    # Load the first 100 jobs with combined text fields
    jobs = con.execute("""
        SELECT id, 
               CONCAT_WS(' ',
                    title,
                    description,
                    skills_desc,
                    formatted_experience_level,
                    location,
                    company_name
                ) AS search_text
        FROM linkedin_jobs
        LIMIT 1000;
    """).fetchall()

    # Assume 'resume.pdf' is your resume file
    resume_text = extract_text_from_pdf("resume.pdf")
    # Preprocess the resume text
    resume_tokens = preprocess_text(resume_text)
    
    # Create and fill the inverted index
    
    from tqdm import tqdm
    if True:
        for id, search_text in tqdm(jobs, desc="Processing jobs"):
            process_text_string(ht, search_text, id)
    
    # Example keyword queries
    #print("Architecture docs:", ht.get("architecture"))
    # Get top matching jobs
    top_jobs = score_jobs(ht, resume_tokens)  # list of (doc_id, score)
    print("Top Jobs", top_jobs)
    # Paginate top jobs
    pages = paginate_results(top_jobs, page_size=10)
    page_data = get_page(con, pages, 1)
    

    # Example output for client
    import json
    if False:
        print(json.dumps({
            "page": page_num,
            "total_pages": len(pages),
            "results": page_data
        }, indent=2))

UPLOAD_FOLDER = 'uploads'
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

uuids = {}
user_pages = {}

@app.route('/job')
def get_job():
    job_id = request.args.get('id')
    if not job_id:
        return jsonify({'error': 'Missing job ID'}), 400

    try:
        job_id = int(job_id)

        result = con.execute("SELECT * FROM linkedin_jobs WHERE id = $1", [job_id]).fetchall()
        columns = [desc[0] for desc in con.description]

        if not result:
            return jsonify({'error': 'Job not found'}), 404

        job = dict(zip(columns, result[0]))
        return jsonify(job)

    except ValueError:
        return jsonify({'error': 'Invalid job ID'}), 400
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@socketio.on('connect')
def handle_connect():
    print(f"Client connected with sid: {request.sid}")
    socketio.emit('assign_sid', {'sid': request.sid}, room=request.sid)

@app.route("/api/get_resume", methods=["POST"])
def get_resume():
    try:
        data = request.get_json()
        print("Received data: ", data)
        device_uuid = data.get("device_uuid")
        output = uuids[device_uuid]
        
        return jsonify(output)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/get_page_api", methods=["POST"])
def get_page_api():
    data = request.get_json()
    print("Matching: ", data)
    sid = data.get("sid")
    device_uuid = data.get("device_uuid")
    print("Matching: ", sid, device_uuid)
    if not sid:
        return jsonify({"error": "Missing sid"}), 400
    if not device_uuid:
        return jsonify({"error": "Missing device_uuid"}), 400
    page = data.get("page")
    if not page:
        return jsonify({"error": "Missing page number"}), 400
    print("Matching: ", sid, page)

    if True:
        pages = user_pages[device_uuid]
        page_data = get_page(con, pages, page)
    #socketio.emit('assign_sid', {'sid': request.sid}, room=request.sid)
    return jsonify({
        "page": page,
        "total_pages": len(pages),
        "results": page_data
    })

@app.route("/api/match_jobs", methods=["POST"])
def match_jobs():
    data = request.get_json()
    print("Matching: ", data)
    sid = data.get("sid")
    device_uuid = data.get("device_uuid")
    person_info = data.get("person_info")
    print("Matching: ", sid, device_uuid)
    if not sid:
        return jsonify({"error": "Missing sid"}), 400
    uploaded_files = data.get("uploadedFiles")

    page = data.get("page")
    if not page:
        return jsonify({"error": "Missing page number"}), 400
    print("Matching: ", sid, uploaded_files, page)
    # Assume 'resume.pdf' is your resume file
    resume_text = ""
    if True:
        if uploaded_files:
            try:
                if uploaded_files.endswith(".pdf"):
                    resume_text = extract_text_from_pdf(f"uploads/{sid}/{uploaded_files}")
            except:
                pass
            
        resume_text += f"{person_info['skills']} {person_info['experience']} {person_info['preferences']}"
        # Preprocess the resume text
        resume_tokens = preprocess_text(resume_text)
        
        uuids[device_uuid] = {
            "resume_tokens": resume_tokens,
            "resume_text" : resume_text,
        }

        print(uuids[device_uuid])
        
        # Example keyword queries
        global ht
        print("Architecture docs:", ht.get("architecture"))
        # Get top matching jobs
        top_jobs = score_jobs(ht, resume_tokens)  # list of (doc_id, score)
        #print("Top Jobs", top_jobs)
        # Paginate top jobs
        pages = paginate_results(top_jobs, page_size=10)
        user_pages[device_uuid] = pages
        page_data = get_page(con, pages, page)
    #socketio.emit('assign_sid', {'sid': request.sid}, room=request.sid)
    return jsonify({
        "page": page,
        "total_pages": len(pages),
        "results": page_data
    })

@app.route('/upload', methods=['POST'])
def upload():
    sid = request.form.get('sid')
    files = request.files.getlist('files')

    if not sid or not files:
        return jsonify({'message': 'Missing sid or files'}), 400

    sid_folder = os.path.join(UPLOAD_FOLDER, secure_filename(sid))
    os.makedirs(sid_folder, exist_ok=True)

    for file in files:
        filename = secure_filename(file.filename)
        file.save(os.path.join(sid_folder, filename))

    return jsonify({'message': f'{len(files)} file(s) saved to {sid_folder}'}), 200

@app.route('/api/ai_recommendation', methods=['POST'])
def ai_recommendation():
    try:
        data = request.get_json()
        prompt = data.get("prompt", "")
        if not prompt:
            return jsonify({"error": "Missing prompt"}), 400

        payload = {
            "contents": [
                {
                    "parts": [{"text": prompt}]
                }
            ],
            "generationConfig": {
                "temperature": 0.9,
                "maxOutputTokens": 1024,
                "topP": 1,
                "topK": 40
            }
        }

        headers = {"Content-Type": "application/json"}
        response = requests.post(GEMINI_API_URL, headers=headers, data=json.dumps(payload))

        if response.ok:
            result = response.json()
            # Defensive check for nested structure
            text = result.get("candidates", [{}])[0].get("content", {}).get("parts", [{}])[0].get("text", "No response")
            return jsonify({"result": text})
        else:
            return jsonify({"error": response.text}), response.status_code

    except Exception as e:
        print("❌ Exception occurred in /api/ai_recommendation:")
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

socketio.run(app, host="0.0.0.0", port=5000, debug=True, use_reloader=False)