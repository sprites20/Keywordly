import React, { useState } from 'react';
import { Container, Grid, Button, Typography, Box, TextField} from '@mui/material';
import { Link } from 'react-router-dom';
import axios from 'axios';

function Home() {
const [query, setQuery] = useState('');
  const [solution, setSolution] = useState('');

  const fetchSolution = async () => {
    try {
      const response = await axios.post('http://localhost:5000/generate-solution', {
        query,
      });
      setSolution(response.data.solution);
    } catch (error) {
      console.error('Error fetching solution:', error);
    }
  };
  return (
    <Box sx={{ backgroundColor: '#f0f4f8', height: '100vh' }}>
      <Container maxWidth="lg" sx={{ paddingTop: 10, textAlign: 'center' }}>
        <Typography variant="h3" gutterBottom sx={{ fontWeight: 'bold', color: '#333' }}>
          Welcome to the Societal Solutions Platform
        </Typography>
        <Typography variant="h6" sx={{ marginBottom: 4, color: '#6c757d' }}>
          Get AI-generated solutions for societal challenges.
        </Typography>
		
		<TextField
		  label="Enter your query"
		  multiline
		  rows={1}
		  fullWidth
		  variant="outlined"
		  value={query}
		  onChange={(e) => setQuery(e.target.value)}
		  sx={{ marginBottom: 3 }}
		/>
		<Button
		  fullWidth
		  variant="outlined"
		  sx={{
			backgroundColor: '#004d40',
			color: '#fff',
			'&:hover': { backgroundColor: '#00796b' },
		  }}
		  component={Link}
		  to={`/category/general?query=${encodeURIComponent(query)}`} // Dynamically set query here
		>
		  Generate Solution
		</Button>
		
        {/* Main Categories */}
<Grid container spacing={3} justifyContent="center">
  {/* Basic Infrastructure */}
  <Grid item xs={12} sm={4}>
    <Box
      sx={{
        backgroundColor: '#e0f2f1',  // Light teal background for the box
        borderRadius: '8px',          // Rounded corners for the box
        padding: '16px',              // Add some padding inside the box
        boxShadow: 2,                 // Optional shadow for depth effect
      }}
    >
      <Button
        fullWidth
        variant="contained"
        sx={{
          backgroundColor: '#00796b',
          '&:hover': { backgroundColor: '#004d40' },
          fontWeight: 'bold',
          padding: '15px',
        }}
        component={Link}
        to="/category/basic-infrastructure"
      >
        Basic Infrastructure
      </Button>
      {/* Subcategories for Basic Infrastructure */}
      <Grid container spacing={2} justifyContent="center" sx={{ marginTop: 2 }}>
        <Grid item xs={12}>
          <Button
            fullWidth
            variant="outlined"
            sx={{
              backgroundColor: '#004d40',
              color: '#fff',
              '&:hover': { backgroundColor: '#00796b' },
            }}
            component={Link}
            to="/category/basic-infrastructure/electricity"
          >
            Electricity
          </Button>
        </Grid>
        <Grid item xs={12}>
          <Button
            fullWidth
            variant="outlined"
            sx={{
              backgroundColor: '#004d40',
              color: '#fff',
              '&:hover': { backgroundColor: '#00796b' },
            }}
            component={Link}
            to="/category/basic-infrastructure/water-supply"
          >
            Water Supply
          </Button>
        </Grid>
        <Grid item xs={12}>
          <Button
            fullWidth
            variant="outlined"
            sx={{
              backgroundColor: '#004d40',
              color: '#fff',
              '&:hover': { backgroundColor: '#00796b' },
            }}
            component={Link}
            to="/category/basic-infrastructure/sanitation-waste-management"
          >
            Sanitation & Waste Management
          </Button>
        </Grid>
        <Grid item xs={12}>
          <Button
            fullWidth
            variant="outlined"
            sx={{
              backgroundColor: '#004d40',
              color: '#fff',
              '&:hover': { backgroundColor: '#00796b' },
            }}
            component={Link}
            to="/category/basic-infrastructure/transportation"
          >
            Transportation
          </Button>
        </Grid>
        <Grid item xs={12}>
          <Button
            fullWidth
            variant="outlined"
            sx={{
              backgroundColor: '#004d40',
              color: '#fff',
              '&:hover': { backgroundColor: '#00796b' },
            }}
            component={Link}
            to="/category/basic-infrastructure/communication-networks"
          >
            Communication Networks
          </Button>
        </Grid>
        <Grid item xs={12}>
          <Button
            fullWidth
            variant="outlined"
            sx={{
              backgroundColor: '#004d40',
              color: '#fff',
              '&:hover': { backgroundColor: '#00796b' },
            }}
            component={Link}
            to="/category/basic-infrastructure/housing"
          >
            Housing
          </Button>
        </Grid>
      </Grid>
    </Box>
  </Grid>

  {/* Economic and Industrial Systems */}
  <Grid item xs={12} sm={4}>
    <Box
      sx={{
        backgroundColor: '#e3f2fd',  // Light blue background for the box
        borderRadius: '8px',
        padding: '16px',
        boxShadow: 2,
      }}
    >
      <Button
        fullWidth
        variant="contained"
        sx={{
          backgroundColor: '#2196f3',
          '&:hover': { backgroundColor: '#1976d2' },
          fontWeight: 'bold',
          padding: '15px',
        }}
        component={Link}
        to="/category/economic-systems"
      >
        Economic & Industrial Systems
      </Button>
      {/* Subcategories for Economic & Industrial Systems */}
      <Grid container spacing={2} justifyContent="center" sx={{ marginTop: 2 }}>
        <Grid item xs={12}>
          <Button
            fullWidth
            variant="outlined"
            sx={{
              backgroundColor: '#1976d2',
              color: '#fff',
              '&:hover': { backgroundColor: '#2196f3' },
            }}
            component={Link}
            to="/category/economic-systems/food-production"
          >
            Food Production
          </Button>
        </Grid>
        <Grid item xs={12}>
          <Button
            fullWidth
            variant="outlined"
            sx={{
              backgroundColor: '#1976d2',
              color: '#fff',
              '&:hover': { backgroundColor: '#2196f3' },
            }}
            component={Link}
            to="/category/economic-systems/energy-resources"
          >
            Energy Resources
          </Button>
        </Grid>
        <Grid item xs={12}>
          <Button
            fullWidth
            variant="outlined"
            sx={{
              backgroundColor: '#1976d2',
              color: '#fff',
              '&:hover': { backgroundColor: '#2196f3' },
            }}
            component={Link}
            to="/category/economic-systems/industry-manufacturing"
          >
            Industry & Manufacturing
          </Button>
        </Grid>
      </Grid>
    </Box>
  </Grid>

  {/* Health & Safety */}
  <Grid item xs={12} sm={4}>
    <Box
      sx={{
        backgroundColor: '#ffebee',  // Light red background for the box
        borderRadius: '8px',
        padding: '16px',
        boxShadow: 2,
      }}
    >
      <Button
        fullWidth
        variant="contained"
        sx={{
          backgroundColor: '#ff5722',
          '&:hover': { backgroundColor: '#e64a19' },
          fontWeight: 'bold',
          padding: '15px',
        }}
        component={Link}
        to="/category/health-and-safety"
      >
        Health & Safety
      </Button>
      {/* Subcategories for Health & Safety */}
      <Grid container spacing={2} justifyContent="center" sx={{ marginTop: 2 }}>
        <Grid item xs={12}>
          <Button
            fullWidth
            variant="outlined"
            sx={{
              backgroundColor: '#e64a19',
              color: '#fff',
              '&:hover': { backgroundColor: '#ff5722' },
            }}
            component={Link}
            to="/category/health-and-safety/healthcare"
          >
            Healthcare System
          </Button>
        </Grid>
        <Grid item xs={12}>
          <Button
            fullWidth
            variant="outlined"
            sx={{
              backgroundColor: '#e64a19',
              color: '#fff',
              '&:hover': { backgroundColor: '#ff5722' },
            }}
            component={Link}
            to="/category/health-and-safety/law-order"
          >
            Law & Order
          </Button>
        </Grid>
        <Grid item xs={12}>
          <Button
            fullWidth
            variant="outlined"
            sx={{
              backgroundColor: '#e64a19',
              color: '#fff',
              '&:hover': { backgroundColor: '#ff5722' },
            }}
            component={Link}
            to="/category/health-and-safety/emergency-services"
          >
            Emergency Services
          </Button>
        </Grid>
      </Grid>
    </Box>
  </Grid>
</Grid>
		
		{/* Main Categories */}
        <Grid container spacing={3} justifyContent="center">
          {/* Social and Cultural Systems */}
          <Grid item xs={12} sm={4}>
            <Box
              sx={{
                backgroundColor: '#f3e5f5',
                borderRadius: '10px',
                padding: 3,
                boxShadow: 3,
              }}
            >
              <Button
                fullWidth
                variant="contained"
                sx={{
                  backgroundColor: '#9c27b0',
                  '&:hover': { backgroundColor: '#7b1fa2' },
                  fontWeight: 'bold',
                  padding: '15px',
                }}
                component={Link}
                to="/category/social-and-cultural-systems"
              >
                Social & Cultural Systems
              </Button>
              {/* Subcategories for Social and Cultural Systems */}
              <Grid container spacing={2} justifyContent="center" sx={{ marginTop: 2 }}>
                <Grid item xs={12}>
                  <Button
                    fullWidth
                    variant="outlined"
                    sx={{
                      backgroundColor: '#7b1fa2',
                      color: '#fff',
                      '&:hover': { backgroundColor: '#9c27b0' },
                    }}
                    component={Link}
                    to="/category/social-and-cultural-systems/education"
                  >
                    Education
                  </Button>
                </Grid>
                <Grid item xs={12}>
                  <Button
                    fullWidth
                    variant="outlined"
                    sx={{
                      backgroundColor: '#7b1fa2',
                      color: '#fff',
                      '&:hover': { backgroundColor: '#9c27b0' },
                    }}
                    component={Link}
                    to="/category/social-and-cultural-systems/social-welfare"
                  >
                    Social Welfare
                  </Button>
                </Grid>
                <Grid item xs={12}>
                  <Button
                    fullWidth
                    variant="outlined"
                    sx={{
                      backgroundColor: '#7b1fa2',
                      color: '#fff',
                      '&:hover': { backgroundColor: '#9c27b0' },
                    }}
                    component={Link}
                    to="/category/social-and-cultural-systems/cultural-recreational-facilities"
                  >
                    Cultural & Recreational Facilities
                  </Button>
                </Grid>
              </Grid>
            </Box>
          </Grid>

          {/* Environmental Management */}
          <Grid item xs={12} sm={4}>
            <Box
              sx={{
                backgroundColor: '#c8e6c9',
                borderRadius: '10px',
                padding: 3,
                boxShadow: 3,
              }}
            >
              <Button
                fullWidth
                variant="contained"
                sx={{
                  backgroundColor: '#388e3c',
                  '&:hover': { backgroundColor: '#2c6b2f' },
                  fontWeight: 'bold',
                  padding: '15px',
                }}
                component={Link}
                to="/category/environmental-management"
              >
                Environmental Management
              </Button>
              {/* Subcategories for Environmental Management */}
              <Grid container spacing={2} justifyContent="center" sx={{ marginTop: 2 }}>
                <Grid item xs={12}>
                  <Button
                    fullWidth
                    variant="outlined"
                    sx={{
                      backgroundColor: '#2c6b2f',
                      color: '#fff',
                      '&:hover': { backgroundColor: '#388e3c' },
                    }}
                    component={Link}
                    to="/category/environmental-management/natural-resource-management"
                  >
                    Natural Resource Management
                  </Button>
                </Grid>
                <Grid item xs={12}>
                  <Button
                    fullWidth
                    variant="outlined"
                    sx={{
                      backgroundColor: '#2c6b2f',
                      color: '#fff',
                      '&:hover': { backgroundColor: '#388e3c' },
                    }}
                    component={Link}
                    to="/category/environmental-management/pollution-control"
                  >
                    Pollution Control
                  </Button>
                </Grid>
              </Grid>
            </Box>
          </Grid>

          {/* Governance and Administration */}
          <Grid item xs={12} sm={4}>
            <Box
              sx={{
                backgroundColor: '#bbdefb',
                borderRadius: '10px',
                padding: 3,
                boxShadow: 3,
              }}
            >
              <Button
                fullWidth
                variant="contained"
                sx={{
                  backgroundColor: '#1976d2',
                  '&:hover': { backgroundColor: '#1565c0' },
                  fontWeight: 'bold',
                  padding: '15px',
                }}
                component={Link}
                to="/category/governance-and-administration"
              >
                Governance & Administration
              </Button>
              {/* Subcategories for Governance and Administration */}
              <Grid container spacing={2} justifyContent="center" sx={{ marginTop: 2 }}>
                <Grid item xs={12}>
                  <Button
                    fullWidth
                    variant="outlined"
                    sx={{
                      backgroundColor: '#1565c0',
                      color: '#fff',
                      '&:hover': { backgroundColor: '#1976d2' },
                    }}
                    component={Link}
                    to="/category/governance-and-administration/government-institutions"
                  >
                    Government Institutions
                  </Button>
                </Grid>
                <Grid item xs={12}>
                  <Button
                    fullWidth
                    variant="outlined"
                    sx={{
                      backgroundColor: '#1565c0',
                      color: '#fff',
                      '&:hover': { backgroundColor: '#1976d2' },
                    }}
                    component={Link}
                    to="/category/governance-and-administration/defense-security"
                  >
                    Defense & Security
                  </Button>
                </Grid>
                <Grid item xs={12}>
                  <Button
                    fullWidth
                    variant="outlined"
                    sx={{
                      backgroundColor: '#1565c0',
                      color: '#fff',
                      '&:hover': { backgroundColor: '#1976d2' },
                    }}
                    component={Link}
                    to="/category/governance-and-administration/financial-systems"
                  >
                    Financial Systems
                  </Button>
                </Grid>
              </Grid>
            </Box>
          </Grid>

          {/* Other Critical Aspects */}
          <Grid item xs={12} sm={4}>
            <Box
              sx={{
                backgroundColor: '#fff3e0',
                borderRadius: '10px',
                padding: 3,
                boxShadow: 3,
              }}
            >
              <Button
                fullWidth
                variant="contained"
                sx={{
                  backgroundColor: '#f57c00',
                  '&:hover': { backgroundColor: '#ef6c00' },
                  fontWeight: 'bold',
                  padding: '15px',
                }}
                component={Link}
                to="/category/other-critical-aspects"
              >
                Other Critical Aspects
              </Button>
              {/* Subcategories for Other Critical Aspects */}
              <Grid container spacing={2} justifyContent="center" sx={{ marginTop: 2 }}>
                <Grid item xs={12}>
                  <Button
                    fullWidth
                    variant="outlined"
                    sx={{
                      backgroundColor: '#ef6c00',
                      color: '#fff',
                      '&:hover': { backgroundColor: '#f57c00' },
                    }}
                    component={Link}
                    to="/category/other-critical-aspects/public-transportation"
                  >
                    Public Transportation
                  </Button>
                </Grid>
                <Grid item xs={12}>
                  <Button
                    fullWidth
                    variant="outlined"
                    sx={{
                      backgroundColor: '#ef6c00',
                      color: '#fff',
                      '&:hover': { backgroundColor: '#f57c00' },
                    }}
                    component={Link}
                    to="/category/other-critical-aspects/urban-planning"
                  >
                    Urban Planning
                  </Button>
                </Grid>
                <Grid item xs={12}>
                  <Button
                    fullWidth
                    variant="outlined"
                    sx={{
                      backgroundColor: '#ef6c00',
                      color: '#fff',
                      '&:hover': { backgroundColor: '#f57c00' },
                    }}
                    component={Link}
                    to="/category/other-critical-aspects/waste-recycling-renewable-energy"
                  >
                    Waste Recycling & Renewable Energy
                  </Button>
                </Grid>
              </Grid>
            </Box>
          </Grid>
        </Grid>

        {/* Add More Categories Below */}
        {/* Social and Cultural Systems */}
        {/* Environmental Management */}
        {/* Governance and Administration */}
        {/* Other Critical Aspects */}
        
      </Container>
    </Box>
  );
}

export default Home;