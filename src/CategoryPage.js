import React, { useEffect, useState } from 'react';
import { Box, TextField, Button, Typography, Container, Paper, Grid } from '@mui/material';
import { useLocation, useSearchParams } from 'react-router-dom';
import axios from 'axios';

function CategoryPage({ category }) {
  const [query, setQuery] = useState('');
  const [solutions, setSolutions] = useState([]);
  const [searchParams] = useSearchParams();
  
  // Extract query from the URL
  useEffect(() => {
    const queryFromUrl = searchParams.get('query');
    if (queryFromUrl) {
      setQuery(queryFromUrl);
      fetchSolutions(queryFromUrl);
    }
  }, [searchParams]);

  const fetchSolutions = async (queryParam) => {
    try {
      const response = await axios.post('http://localhost:5000/generate-solution', {
        query: queryParam || query,
        category,
      });
      setSolutions(response.data.solutions || []);
    } catch (error) {
      console.error('Error fetching solutions:', error);
    }
  };

  return (
    <Box sx={{ backgroundColor: '#f0f4f8', height: '100vh', paddingTop: 8 }}>
      <Container maxWidth="md">
        <Paper sx={{ padding: 4, borderRadius: 2, boxShadow: 3 }}>
          <Typography variant="h4" gutterBottom sx={{ fontWeight: 'bold', color: '#00796b' }}>
            {category} Documents
          </Typography>
          <TextField
            label="Enter your query"
            multiline
            rows={4}
            fullWidth
            variant="outlined"
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            sx={{ marginBottom: 3 }}
          />
          <Button
            fullWidth
            variant="contained"
            sx={{ backgroundColor: '#00796b', '&:hover': { backgroundColor: '#004d40' } }}
            onClick={() => fetchSolutions(query)}
          >
            Generate Solutions
          </Button>
        </Paper>
        {solutions.length > 0 && (
          <Box sx={{ marginTop: 4 }}>
            <Typography variant="h5" sx={{ fontWeight: 'bold', color: '#2196f3' }}>
              Solutions:
            </Typography>
            <Grid container spacing={2}>
              {solutions.map((solution, index) => (
                <Grid item xs={12} key={index}>
                  <Paper sx={{ padding: 2, backgroundColor: '#ffffff', borderRadius: 2, boxShadow: 2 }}>
                    <Typography>{solution}</Typography>
                  </Paper>
                </Grid>
              ))}
            </Grid>
          </Box>
        )}
      </Container>
    </Box>
  );
}

export default CategoryPage;
