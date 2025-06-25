import React from 'react';
import { Link } from 'react-router-dom';
import { AppBar, Toolbar, Typography, Button, Box } from '@mui/material';

function Navbar() {
  return (
    <AppBar position="sticky" sx={{ backgroundColor: '#2E3B55' }}>
      <Toolbar>
        <Typography variant="h6" sx={{ flexGrow: 1 }}>
          Societal Solutions
        </Typography>
        <Box>
          <Button color="inherit" component={Link} to="/category/basic-infrastructure">Basic Infrastructure</Button>
          <Button color="inherit" component={Link} to="/category/economic-systems">Economic Systems</Button>
          <Button color="inherit" component={Link} to="/category/health-and-safety">Health & Safety</Button>
          {/* Add other categories as needed */}
        </Box>
      </Toolbar>
    </AppBar>
  );
}

export default Navbar;
