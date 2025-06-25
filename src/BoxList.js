import React, { useState, useEffect } from 'react';
import './BoxList.css';
import io from 'socket.io-client';

// Dummy job data for demonstration
var jobData = Array.from({ length: 50 }, (_, i) => ({
  id: i + 1,
  title: `Job Title ${i + 1}: Software Engineer`,
  description: `This is a detailed description for Job ${i + 1}. We are looking for a passionate software engineer to join our dynamic team. Responsibilities include developing and maintaining web applications, collaborating with cross-functional teams, and contributing to all phases of the development lifecycle. Experience with React, Node.js, and cloud platforms is a plus. This role offers excellent growth opportunities and a collaborative work environment. Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat.`,
  location: `City ${Math.floor(Math.random() * 5) + 1}, State`,
  // Add more details as needed
}));

const truncateDescription = (text, maxLength) => {
  if (text.length <= maxLength) return text;
  return text.substring(0, maxLength) + '...';
};

const BoxList = () => {
  const jobsPerPage = 10; // jobs to show per page
  const [currentPage, setCurrentPage] = useState(1);
  const [uploadedFiles, setUploadedFiles] = useState([]); // State to store uploaded files for the whole list
  const [skills, setSkills] = useState('');
  const [experience, setExperience] = useState('');
  const [preferences, setPreferences] = useState('');
  const [visibleJobs, setVisibleJobs] = useState([]);
  const [isMatching, setIsMatching] = useState(false);
  const [totalPages, setTotalPages] = useState(0);
  const [sid, setSid] = useState(null);
  const [socket, setSocket] = useState(null);
  const startIdx = (currentPage - 1) * jobsPerPage;
  const [isMatchedJobs, setMatchedJobs] = useState(false);
  const [deviceId, setDeviceId] = useState("");
  //visibleJobs = jobData.slice(startIdx, startIdx + jobsPerPage);

  const get_page = async () => {
    setIsMatching(true);
    setVisibleJobs([]);
    try {
      const payload = {
        sid: sid,
        device_uuid: deviceId,
        page: currentPage,
      };
      const response = await fetch("http://localhost:5000/api/get_page_api", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      
      const result = await response.json();
      console.log(result);
      setTotalPages(result.total_pages);
      const matchedJobs = result.results.map((job, index) => ({
        id: job.doc_id || index + 1,
        title: job.title || "No Title Provided", // or any placeholder title
        description: job.snippet || "No description provided.",
        location: job.location || "Unknown", // or from `job` if available
        company_name: job.company_name || "Unknown" // or from `job` if available
      }));
      
      setVisibleJobs(matchedJobs);
      setIsMatching(false);
      setMatchedJobs(true);
    } catch (error) {
      console.error("Job match error:", error);
      setMatchedJobs(false);
    } finally {
      setIsMatching(false);
    }
  };

  const handleNext = () => {
    if (currentPage < totalPages) setCurrentPage(currentPage + 1);
    get_page();
  };

  const handlePrev = () => {
    if (currentPage > 1) setCurrentPage(currentPage - 1);
    get_page();
    //Request that page in the server
  };

  const handleJobClick = (jobId) => {
    window.open(`/job?id=${jobId}`, '_blank');
    // In a real application, you would navigate to a job detail page
  };

  const handleDragOver = (event) => {
    event.preventDefault(); // Prevent default to allow drop
  };
  

  useEffect(() => {
    const socket = io("http://localhost:5000"); // adjust if hosted differently
    setSocket(socket);
    socket.on("connect", () => {
      console.log("Connected to socket server.");
    });

    socket.on("assign_sid", (data) => {
      console.log("SID assigned:", data.sid);
      setSid(data.sid);
    });

    // Clean up socket on unmount
    return () => socket.disconnect();
  }, []);

  // Utility functions for cookies
  function setCookie(name, value, days = 365) {
    const expires = new Date(Date.now() + days * 864e5).toUTCString();
    document.cookie = `${name}=${encodeURIComponent(value)}; expires=${expires}; path=/`;
  }

  function getCookie(name) {
    return document.cookie
      .split("; ")
      .reduce((r, v) => {
        const parts = v.split("=");
        return parts[0] === name ? decodeURIComponent(parts[1]) : r;
      }, "");
  }

  function generateUUID() {
    return crypto.randomUUID(); // use polyfill if needed
  }
  
  // Set device UUID cookie on first load
  useEffect(() => {
    let uuid = getCookie("device_uuid");
    if (!uuid) {
      uuid = generateUUID();
      setCookie("device_uuid", uuid);
    }
    setDeviceId(uuid);
    console.log("Device UUID:", deviceId);
  }, []);

  useEffect(() => {
    if (deviceId) {
      console.log("Device UUID (from state):", deviceId);
    }
  }, [deviceId]);

  const handleDrop = async (event) => {
    event.preventDefault();
    const files = Array.from(event.dataTransfer.files);

    if (!sid) {
      alert("Not connected to server. Try again.");
      return;
    }

    if (files.length > 0) {
      setUploadedFiles(files.map(file => file.name));
      alert(`File(s) dropped: ${files.map(file => file.name).join(', ')}`);

      const formData = new FormData();
      formData.append("sid", sid);
      formData.append("device_uuid", deviceId);
      files.forEach(file => formData.append("files", file));

      try {
        const response = await fetch("http://localhost:5000/upload", {
          method: "POST",
          body: formData
        });

        const result = await response.json();
        alert(result.message);
      } catch (error) {
        console.error("Upload failed:", error);
        alert("Upload failed.");
      }
    }
  };

  const handleFileSelect = (event) => {
    const files = Array.from(event.target.files);
    if (files.length > 0) {
      setUploadedFiles(files.map(file => file.name));
      alert(`File(s) selected: ${files.map(file => file.name).join(', ')}`);
    }
  };

  const triggerFileInput = () => {
    document.getElementById('fileInput').click();
  };
  const triggerBack = () => {
    setMatchedJobs(false);
  };
  const triggerMatchJobs = async (page) => {
    setIsMatching(true);
    setVisibleJobs([]);
    const pageNum = 1;
    try {
      const payload = {
        sid: sid,
        device_uuid: deviceId,
        page: pageNum,
        uploadedFiles: uploadedFiles[0] // Assuming you're sending files
        // Remove uploadedFiles if you're not sending files
      };
      const response = await fetch("http://localhost:5000/api/match_jobs", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      
      const result = await response.json();
      console.log(result);
      setTotalPages(result.total_pages);
      setCurrentPage(1);
      const matchedJobs = result.results.map((job, index) => ({
        id: job.doc_id || index + 1,
        title: job.title || "No Title Provided", // or any placeholder title
        description: job.snippet || "No description provided.",
        location: job.location || "Unknown", // or from `job` if available
        company_name: job.company_name || "Unknown" // or from `job` if available
      }));
      
      setVisibleJobs(matchedJobs);
      setIsMatching(false);
      setMatchedJobs(true);
    } catch (error) {
      console.error("Job match error:", error);
      setMatchedJobs(false);
    } finally {
      setIsMatching(false);
    }
  };
  


  const triggerMatchJobs1 = () => {
    setIsMatching(true); // show "Matching jobs..." message
    setVisibleJobs([]); // clear current jobs
    
    socket.emit("match_jobs", {
      username: "alice",
      job_preferences: ["design", "remote"],
      location: "Tokyo"
    });
    // Simulate job matching (e.g., fetching from backend)
    setTimeout(() => {
      const matchedJobs = Array.from({ length: 50 }, (_, i) => ({
        id: i + 1,
        title: `Job Title ${i + 1}: Software Engineer`,
        description: `This is a detailed description for Job ${i + 1}. We are looking for a passionate software engineer to join our dynamic team. Responsibilities include developing and maintaining web applications, collaborating with cross-functional teams, and contributing to all phases of the development lifecycle. Experience with React, Node.js, and cloud platforms is a plus. This role offers excellent growth opportunities and a collaborative work environment. Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat.`,
        location: `City ${Math.floor(Math.random() * 5) + 1}, State`,
        // Add more details as needed
      }));
      
      setTotalPages(Math.ceil(matchedJobs.length / jobsPerPage));
      setVisibleJobs(matchedJobs);
      setIsMatching(false);
    }, 2000); // 2-second delay to simulate loading
  };

  return (
    <div className="box-list-container">
      {!isMatchedJobs && <div className="application-form">
        <h2>Your Application Details</h2>
        <div className="form-group">
          <label htmlFor="skills">Skills:</label>
          <input 
            type="text" 
            id="skills" 
            value={skills} 
            onChange={(e) => setSkills(e.target.value)} 
            placeholder="e.g., React, Node.js, AWS"
          />
        </div>
        <div className="form-group">
          <label htmlFor="experience">Experience (Years):</label>
          <input 
            type="number" 
            id="experience" 
            value={experience} 
            onChange={(e) => setExperience(e.target.value)} 
            placeholder="e.g., 5"
          />
        </div>
        <div className="form-group">
          <label htmlFor="preferences">Preferences:</label>
          <textarea 
            id="preferences" 
            value={preferences} 
            onChange={(e) => setPreferences(e.target.value)} 
            placeholder="e.g., Remote, Full-time, specific industries"
          ></textarea>
        </div>
      </div>
      }

      {!isMatchedJobs && <div 
        className="overall-drop-area"
        onDragOver={handleDragOver}
        onDrop={handleDrop}
      >
        Drag & Drop Your Resume/Files Here
        <p>or</p>
        <button onClick={triggerFileInput} className="select-file-button">
          Select Files
        </button>
        <input 
          type="file" 
          id="fileInput" 
          style={{ display: 'none' }} 
          onChange={handleFileSelect} 
          multiple 
        />
        {uploadedFiles.length > 0 && (
          <p className="overall-uploaded-file-info">Uploaded: {uploadedFiles.join(', ')}</p>
        )}
      </div>
      }
      {!isMatchedJobs && <div className="center-button-container">
      <button onClick={triggerMatchJobs} className="match-jobs-button">
          Match Jobs
      </button>
      </div>
      }

      {isMatchedJobs && <div className="center-button-container">
      <button onClick={triggerBack} className="match-jobs-button">
          Back
      </button>
      </div>
      }
      {isMatchedJobs && <div className="boxes-frame">
        {visibleJobs.map((job) => (
          <div key={job.id} className="box" onClick={() => handleJobClick(job.id)}>
            <h3 className="job-title">{job.title}</h3>
            <p className="job-description">{truncateDescription(job.description, 500)}</p>
            <p className="job-location">Location: {job.location}</p>
            <p className="job-location">Company: {job.company_name}</p>
            {/* Add more job details here */}
          </div>
        ))}
      </div>
      }

      {isMatchedJobs && <div className="pagination">
        <button
          onClick={handlePrev}
          disabled={currentPage === 1}
        >
          Previous
        </button>
        <span>Page {currentPage} of {totalPages}</span>
        <button
          onClick={handleNext}
          disabled={currentPage === totalPages}
        >
          Next
        </button>
      </div>
      }
    </div>
  );
};

export default BoxList;
