# Datasets

In this folder we store the datasets we used during our experiments.

Each dataset contains 2-d points, forming 5 polygons in a plane; 4 large and a small one.

## data_size1

- `data_size1` folder:
    - Contains 100 duplicate files
    - Each file contains 5725 points
    - The points are the exact same for each file
- `data_size1_w_outliers` file:
    - Contains the original 5725 points plus outliers
    - Outliers form a circle around the 5 polygons, with a radius of 2.9
- `data1_<cardinality multiple>` files:
    - Contain 5725 * \<cardinality multiple\> non-duplicate points
    - The first 5725 points are the exact same points as the original 5725
    - The rest are copies of these points with a random amount added to them from a uniform distribution
    - The shape of the data has remained largely intact
- `data1_<cardinality multiple>_w_outliers` files:
    - Contain the exact same points as `data1_<cardinality multiple>` plus outliers
    - Outliers are a fraction of the number of points of the dataset (0.0003 * cardinality)
    - Outliers form a circle around the 5 polygons, with a radius of 2.9

## data_size2

Similar to data_size1, with a higher number of points

- `data_size2` folder:
  - Contains 100 duplicate files
  - Each file contains 16924 points
  - The points are the exact same for each file
- `data_size2_w_outliers` file:
  - Contains the original 16924 points plus outliers
  - Outliers form a circle around the 5 polygons, with a radius of 2.9
- `data2_<cardinality multiple>` files:
  - Contain 16924 * \<cardinality multiple\> non-duplicate points
  - The first 16924 points are the exact same points as the original 16924
  - The rest are copies of these points with a random amount added to them from a uniform distribution
  - The shape of the data has remained largely intact
- `data2_<cardinality multiple>_w_outliers` files:
  - Contain the exact same points as `data2_<cardinality multiple>` plus outliers
  - Outliers are a fraction of the number of points of the dataset (0.0003 * cardinality)
  - Outliers form a circle around the 5 polygons, with a radius of 2.9
