from ccdexplorer.ccdexplorer_mcp.core import create_mcp


def main() -> None:
    mcp = create_mcp()
    mcp.run()


if __name__ == "__main__":
    main()
