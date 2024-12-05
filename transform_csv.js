const fs = require('fs');
const csv = require('csv-parser');
const { parse } = require('json2csv');

// Middle layer mapping
const headerMapping = {
  "user_id": "user_id",
  "first name": "first_name",
  "last name": "last_name",
  "phone": "phone",
  "email address": "email",
  "gender": "gender",
  "Age_Category": "age_category",
  "Country Residence": "country",
  "Birth date": "birthdate",
  "Registration Date": "reg_date",
};

// Function to transform date format to YYYY-MM-DD
function transformDate(date) {
  if (!date) return ''; // Handle empty or invalid dates
  const [datePart, timePart] = date.split(' ');
  const parts = datePart.split('-');
  if (parts.length === 3) {
    const [day, month, year] = parts;
    return `${year}-${month}-${day}${timePart ? ` ${timePart}` : ''}`;
  }
  return ''; // Return empty if the format is unexpected
}

// Input and output file paths
const inputFilePath = './sample_users_data.csv';
const outputFilePath = './transformed_users_data.csv';

// Array to store transformed rows
const rows = [];
fs.createReadStream(inputFilePath)
  .pipe(csv())
  .on('data', (row) => {
    try {
      const transformedRow = {};

      // Map and transform fields
      for (const [currentHeader, newHeader] of Object.entries(headerMapping)) {
        // Trim headers and keys for matching
        const trimmedRow = {};
        for (const key in row) {
          trimmedRow[key.trim()] = row[key];
        }

        if (currentHeader in trimmedRow) {
          let value = trimmedRow[currentHeader];
          // Handle date transformation
          if (newHeader === "birthdate" || newHeader === "reg_date") {
            value = transformDate(value);
          }
          transformedRow[newHeader] = value || "N/A"; // Default for missing fields
        }
      }
      rows.push(transformedRow);
    } catch (error) {
      console.error(`Error transforming row: ${error.message}`);
    }
  })
  .on('end', () => {
    try {
      // Write transformed data to a new CSV
      const csvString = parse(rows, { fields: Object.values(headerMapping) });
      fs.writeFileSync(outputFilePath, csvString, 'utf8');
      console.log(`Transformed CSV saved to ${outputFilePath}`);
    } catch (error) {
      console.error(`Error writing CSV: ${error.message}`);
    }
  })
  .on('error', (err) => {
    console.error(`Error processing CSV: ${err.message}`);
  });
