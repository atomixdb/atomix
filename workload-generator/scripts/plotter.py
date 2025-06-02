import warnings
from typing import List

import plotly.io as pio
import plotly.graph_objects as go
from plotly.subplots import make_subplots

warnings.filterwarnings("ignore", category=DeprecationWarning)

pio.kaleido.scope.mathjax = None


def make_plots(
    figs,
    rows,
    cols,
    axis_title_font_size,
    axis_tick_font_size,
    output_path,
    height=None,
    width=None,
    titles=None,
    column_widths=None,
    showlegend=True,
    legend_title=None,
    title=None,
) -> go.Figure:
    """Make plots from the given figures"""

    fig: go.Figure = make_subplots(
        rows=rows, cols=cols, subplot_titles=titles, column_widths=column_widths
    )
    # Add the figures to the subplots
    for i, row in enumerate(figs):
        for j, (func, args) in enumerate(row):
            trace = func(**args)
            if isinstance(trace, list):
                for t in trace:
                    fig.add_trace(t, row=i + 1, col=j + 1)
            else:
                fig.add_trace(trace, row=i + 1, col=j + 1)
            fig.update_xaxes(
                title=args.get("x_axis_title"),
                tickfont={"size": axis_tick_font_size.get("x")},
                title_font={"size": axis_title_font_size.get("x")},
                showgrid=True,
                range=args.get("x_range"),
                row=i + 1,
                col=j + 1,
            )
            fig.update_yaxes(
                title=args.get("y_axis_title"),
                title_standoff=0,
                tickfont={"size": axis_tick_font_size.get("y")},
                title_font={"size": axis_title_font_size.get("y")},
                showgrid=True,
                range=args.get("y_range"),
                row=i + 1,
                col=j + 1,
            )

    fig.update_layout(
        legend={
            # "title": None,
            # "font": {"size": 20},
            # **legend,
        },
        template="simple_white",
        showlegend=showlegend,
        height=height,
        width=width,
        barmode="group",
        title=title,
        # boxmode="group",
        # margin=dict(t=0, b=0),
    )
    # fig.show()
    fig.write_image(f"{output_path}.png", engine="kaleido")
    return fig


def get_unique_key_values(df, key):
    """Get the unique values of the key"""
    return df[key].unique()


def line(df, x, y, key=None, showlegend=True, color_map=None, marker_map=None, **kwargs) -> go.Scatter:
    """Create a line plot"""

    if key is None:
        traces = [
            go.Scatter(
                x=df[x],
                y=df[y],
                # legendgroup=key,
                name=key,
                marker_color=color_map[key],
                marker_symbol=marker_map[key],
                marker_size=10,
                showlegend=showlegend,
                mode="lines+markers",
            )
        ]
    else:
        unique_keys = get_unique_key_values(df, key)
        unique_keys = sorted(unique_keys)
        traces = []
        for i, unique_key in enumerate(unique_keys):
            df_key = df[df[key] == unique_key]
            trace = go.Scatter(
                x=df_key[x],
                y=df_key[y],
                name=unique_key,
                legendgroup=unique_key,
                showlegend=showlegend,
                legendgrouptitle=dict(text=key) if i == 0 else None,
                mode="lines+markers",
                marker_color=color_map[unique_key],
                marker_symbol=marker_map[unique_key],
                marker_size=10
            )
            traces.append(trace)
    return traces


def bar(x, y, showlegend=True, **kwargs) -> go.Bar:
    """Create a bar plot"""
    trace = go.Bar(
        y=y,
        x=x,
        orientation="h",
        marker_color="#3182bd",
        showlegend=showlegend,
    )
    return trace


def heatmap(z, x=None, y=None, showlegend=True, **kwargs) -> go.Heatmap:
    """Create a heatmap plot"""
    trace = go.Heatmap(
        z=z,
        x=x,
        y=y,
        colorscale="Blues",
        showscale=False,
        showlegend=showlegend,
    )
    return trace


def box(x, y, name, showlegend=True, **kwargs) -> go.Box:
    """Create a box plot"""
    trace = go.Box(x=x, y=y, name=name, showlegend=showlegend)
    return trace


def cdf(
    cumulative_probabilities, values, showlegend=True, **kwargs
) -> List[go.Scatter]:
    """Create a cdf plot"""
    traces: list = []
    traces.append(
        go.Scatter(
            x=cumulative_probabilities,
            y=values,
            # legendgroup=workload,
            showlegend=showlegend,
            mode="lines",
        )
    )
    return traces
