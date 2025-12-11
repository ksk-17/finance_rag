import React from "react";
import { TextField, InputAdornment, IconButton } from "@mui/material";
import SearchIcon from "@mui/icons-material/Search";

function SearchField({ value, onChange, placeholder = "Search..." }) {
  return (
    <TextField
      value={value}
      onChange={(e) => onChange(e.target.value)}
      placeholder={placeholder}
      variant="outlined"
      size="small"
      fullWidth
      sx={{
        maxWidth: 300,
        "& .MuiOutlinedInput-root": {
          borderRadius: 3,
          backgroundColor: "#fff",
        },
        marginBottom: "10px",
      }}
      InputProps={{
        startAdornment: (
          <InputAdornment position="start">
            <IconButton size="small">
              <SearchIcon color="action" />
            </IconButton>
          </InputAdornment>
        ),
      }}
    />
  );
}

export default SearchField;