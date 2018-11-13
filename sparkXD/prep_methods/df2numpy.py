import numpy as np


# Methods to convert a Dataframe to a local numpy ndarray,
# from portion of spark_sklearn (https://github.com/databricks/spark-sklearn)
# only intended for small files

# prep_methods data types
def analyze_element(x):
    if type(x) is float:
        return (x, np.double)
    if type(x) is int:
        return (x, np.int)
    if type(x) is long:
        return (x, np.long)
    if type(x) is DenseVector:
        return (x.toArray(), (np.double, len(x.toArray())))
    raise ValueError("The type %s could not be understood. Element was %s" % (type(x), x))


def analyze_df(df):
    """ Converts a dataframe into a numpy array.
    """
    rows = df.collect()
    conversions = [[analyze_element(x) for x in row] for row in rows]
    types = [t for d, t in conversions[0]]
    data = [tuple([d for d, t in labeled_elts]) for labeled_elts in conversions]
    names = list(df.columns)
    dt = np.dtype({'names': names, 'formats': types})
    arr = np.array(data, dtype=dt)
    return arr


def df_to_numpy(df, *args):
    """ Converts a dataframe into a (local) numpy array. Each column is named after the same
    column name in the data frame.

    The varargs provide (in order) the list of columns to extract from the dataframe.
    If none are provided, all the columns from the dataframe are extracted.

    This method only handles basic numerical types, or dense vectors with the same length.

    Note: it is not particularly optimized, do not push it too hard.

    :param df: a pyspark.sql.DataFrame object
    :param args: a list of strings that are column names in the dataframe
    :return: a structured numpy array with the content of the data frame.

    Example:
    >>> z = conv.df_to_numpy(df)
    >>> z['x'].dtype, z['x'].shape
    >>> z = conv.df_to_numpy(df, 'y')
    >>> z['y'].dtype, z['y'].shape
    """
    column_names = df.columns
    if not args:
        args = column_names
    column_nameset = set(column_names)
    for name in args:
        assert name in column_nameset, (name, column_names)
    # Just get the interesting columns
    projected = df.select(*args)

    return analyze_df(projected)