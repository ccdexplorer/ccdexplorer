import marimo

__generated_with = "0.16.5"
app = marimo.App(width="full")


@app.cell
def _():
    from ccdexplorer.mongodb import MongoDB
    return (MongoDB,)


@app.cell
def _(MongoDB):
    mongodb = MongoDB(None)
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
