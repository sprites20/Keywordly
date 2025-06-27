import React from 'react';
import { BrowserRouter as Router, Routes, Route } from 'react-router-dom';
import BoxList from './BoxList';
import JobViewer from './JobViewer'; // a component that shows job by ?id
import './App.css';

function App() {
  return (
    <Router>
      <div className="App">
        <div className="app-container">
          <Routes>
            <Route path="/" element={<BoxList />} />
            <Route path="/job" element={<JobViewer />} /> {/* e.g., /job?id=123 */}
          </Routes>
        </div>
      </div>
    </Router>
  );
}

export default App;
