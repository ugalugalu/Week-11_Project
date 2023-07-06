import unittest
import pandas as pd

def extract_data(file_path):
    # Read data from file
    data = pd.read_csv(file_path)
    return data

def transform_data(data):
    # Perform data transformation logic
    data['billing_amount'] = data['billing_amount'].str.replace('$', '').astype(float)
    data['total_charges'] = data['billing_amount'] + data['tax_amount']
    return data

def load_data(data, output_file):
    # Write data to output file
    data.to_csv(output_file, index=False)

class ETLTestCase(unittest.TestCase):
    def setUp(self):
        # Set up test data and files
        self.input_file = 'input_test.csv'
        self.output_file = 'output_test.csv'
        self.expected_output_file = 'expected_output_test.csv'

    def tearDown(self):
        # Clean up test files
        import os
        if os.path.exists(self.input_file):
            os.remove(self.input_file)
        if os.path.exists(self.output_file):
            os.remove(self.output_file)
        if os.path.exists(self.expected_output_file):
            os.remove(self.expected_output_file)

    def test_etl_process(self):
        # Read input data from file
        input_data = extract_data(self.input_file)

        # Transform data
        transformed_data = transform_data(input_data)

        # Write transformed data to output file
        load_data(transformed_data, self.output_file)

        # Read the expected output data from file
        expected_output = extract_data(self.expected_output_file)
        expected_output['billing_amount'] = expected_output['billing_amount'].astype(float)
        expected_output['total_charges'] = expected_output['total_charges'].astype(float)

        # Read the loaded data from the output file
        loaded_data = extract_data(self.output_file)
        

        # Assert the loaded data matches the expected output
        pd.testing.assert_frame_equal(loaded_data, expected_output)

if __name__ == '__main__':
    unittest.main()
