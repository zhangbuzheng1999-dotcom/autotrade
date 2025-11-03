# chart_manager.py
from pyecharts.charts import Kline, Line, Bar, Grid, Tab
from pyecharts.components import Table
from pyecharts import options as opts
import pandas as pd
from pyecharts.options import ComponentTitleOpts


# 图表组件构造函数
def create_kline(df):
    """
    创建K线图（主图）

    参数:
        df (pd.DataFrame): 必须包含 'date', 'open', 'high', 'low', 'close' 列

    返回:
        pyecharts.charts.Kline 对象
    """
    if not isinstance(df, pd.DataFrame):
        raise TypeError("df must be a pandas DataFrame")

    required_cols = {"date", "open", "high", "low", "close"}
    if not required_cols.issubset(df.columns):
        raise ValueError(f"df must have columns {required_cols}")

    date = df["date"].tolist()
    data = df[["open", "close", "low", "high"]].values.tolist()
    return Kline().add_xaxis(date).add_yaxis("K线", data)


def create_line(x_data: list | pd.Series, y_data: list | pd.Series, x_label=None, y_label=None):
    """
    创建折线图（可用于技术指标）

    参数:
        x_data: x轴数据（list或Series）
        y_data: y轴数据（list或Series）
        x_label: x轴标签名（可选）
        y_label: y轴标签名（可选）

    返回:
        pyecharts.charts.Line 对象
    """
    if isinstance(x_data, pd.Series):
        x_data = x_data.tolist()
    elif not isinstance(x_data, list):
        raise TypeError("x_data must be a pandas Series or list")

    if isinstance(y_data, pd.Series):
        y_data = y_data.tolist()
    elif not isinstance(y_data, list):
        raise TypeError("y_data must be a pandas Series or list")

    line = Line().add_xaxis(x_data).add_yaxis(
        y_label,
        y_data,
        symbol="none",
        label_opts=opts.LabelOpts(is_show=False)
    )
    return line


def create_bar(x_data: list, y_data: list, x_label=None, y_label=None):
    """
    创建柱状图（常用于成交量或MACD柱）

    参数:
        x_data: x轴数据（list或Series）
        y_data: y轴数据（list或Series）
        x_label: x轴标签名（可选）
        y_label: y轴标签名（可选）

    返回:
        pyecharts.charts.Bar 对象
    """
    if isinstance(x_data, pd.Series):
        x_data = x_data.tolist()
    elif not isinstance(x_data, list):
        raise TypeError("x_data must be a pandas Series or list")

    if isinstance(y_data, pd.Series):
        y_data = y_data.tolist()
    elif not isinstance(y_data, list):
        raise TypeError("y_data must be a pandas Series or list")

    bar = (
        Bar()
        .add_xaxis(x_data)
        .add_yaxis(
            series_name=y_label,
            y_axis=y_data,
            label_opts=opts.LabelOpts(is_show=False)
        )
    )
    return bar


def create_table(df:pd.DataFrame, title=''):
    if not isinstance(df, pd.DataFrame):
        raise TypeError("df must be a pandas DataFrame")

    headers = df.columns.tolist()
    rows = df.values.tolist()
    t = Table()
    t.add(headers, rows)
    t.set_global_opts(title_opts=ComponentTitleOpts(title=title))
    return t


class ChartManager:
    """
    图表布局管理器 ChartManager
    --------------------------
    用于构建金融图表中常见的主图（K线图）和副图（MACD、成交量等）布局，
    支持技术指标叠加、图表联动、缩放控制、样式统一等功能。

    成员变量:
        - main_chart: 主图（如K线图）
        - overlap_charts: 叠加在主图上的图层（如均线、布林带）
        - sub_charts: 副图列表，每个为 (chart对象, x轴日期列表)
        - width, height: 图表宽高
        - theme: 主题色，默认为白色
        - datazoom_range: 初始缩放区间范围（百分比）
    """

    def __init__(self, width="2460px", height="1300px", theme="white", vgap_pct=6):
        self.main_chart = None  # 主图
        self.overlap_charts = []  # 主图上叠加的图表（如均线）
        self.sub_charts = []  # 副图，每项为 (chart, x_data)
        self.width = width
        self.height = height
        self.theme = theme
        self.datazoom_range = (80, 100)
        self.main_height = 50  # 主图占比
        self.vgap_pct = float(vgap_pct)

    def set_main(self, chart, height=50):
        """设置主图（如K线图）"""
        self.main_chart = chart
        self.main_height = height

    def add_to_main(self, chart):
        """将技术指标等图层叠加到主图上"""
        self.overlap_charts.append(chart)

    def add_sub_chart(self, chart):
        """
        添加副图（下方图层，如MACD柱、成交量）

        参数:
            chart: pyecharts 图表对象（如 Bar、Line）
            x_data: 日期列表（用于绑定副图的 category 类型 x 轴）
        """
        self.sub_charts.append(chart)

    def set_datazoom_range(self, start=80, end=100):
        """
        设置缩放显示的初始区间，百分比范围 [0, 100]
        """
        self.datazoom_range = (start, end)

    def render(self, path="kline.html"):
        """渲染图表到 HTML 文件"""
        self._compose().render(path)

    def output(self):
        """返回图表对象，可用于嵌入 Jupyter Notebook 或 Web 应用"""
        return self._compose()

    def _compose(self):
        """
        构建最终的图表组合布局（主图 + 副图），添加缩放、联动、坐标绑定等
        """
        # 初始化网格布局 Grid
        grid = Grid(init_opts=opts.InitOpts(width=self.width, height=self.height, theme=self.theme))

        # 主图叠加其他图层（如 SMA、BBands）
        base = self.main_chart
        for chart in self.overlap_charts:
            base = base.overlap(chart)

        # 配置全局缩放工具，xaxis_index 包含主图和所有副图
        total_xaxis_indices = list(range(1 + len(self.sub_charts)))  # [0, 1, 2, ...]
        base.set_global_opts(
            datazoom_opts=[
                # 内部缩放
                opts.DataZoomOpts(
                    is_show=False,
                    type_="inside",
                    xaxis_index=total_xaxis_indices,
                    range_start=self.datazoom_range[0],
                    range_end=self.datazoom_range[1],
                ),
                # 滑块缩放
                opts.DataZoomOpts(
                    is_show=True,
                    type_="slider",
                    xaxis_index=total_xaxis_indices,
                    pos_top="96%",
                    range_start=self.datazoom_range[0],
                    range_end=self.datazoom_range[1],
                ),
            ],
            axispointer_opts=opts.AxisPointerOpts(
                is_show=True,
                link=[{"xAxisIndex": "all"}],  # 主图副图联动
                label=opts.LabelOpts(background_color="#777"),
            ),
            tooltip_opts=opts.TooltipOpts(
                trigger="axis",
                axis_pointer_type="cross",
                background_color="rgba(245, 245, 245, 0.8)",
                border_width=1,
                border_color="#ccc",
                textstyle_opts=opts.TextStyleOpts(color="#000"),
            ),
            # 这里新增的 y 轴自适应配置
            yaxis_opts=opts.AxisOpts(
                is_scale=True,
                min_="dataMin",
                max_="dataMax",
                splitline_opts=opts.SplitLineOpts(is_show=False),
            ),
        )
        # 添加主图到布局中（上半部分）
        grid.add(base,
                 grid_opts=opts.GridOpts(pos_top="5%", height=f'{self.main_height}%', pos_left="3%", pos_right="1%"))

        # 添加副图（下半部分），按比例依次向下排布
        num_sub = len(self.sub_charts)
        if num_sub > 0:
            vgap_pct = self.vgap_pct * num_sub  # 预留的图间隔
            sub_height = int((100 - vgap_pct - self.main_height - 4) / num_sub)  # 每个副图占用空间百分比
            cursor_top = self.main_height  # 当前图表未知

            for i, (sub) in enumerate(self.sub_charts):
                x_index = i + 1  # 主图是x轴索引0，副图从1开始
                top = cursor_top + self.vgap_pct  # 副图顶部位置百分比
                cursor_top += sub_height  # 更新下一个图表位置

                # 强制绑定 xaxis/yaxis index，确保和主图联动并显示正确的横轴
                sub.options["xaxis_index"] = x_index
                sub.options["yaxis_index"] = x_index

                sub.set_global_opts(
                    xaxis_opts=opts.AxisOpts(
                        type_="category",
                        grid_index=x_index,
                        boundary_gap=False,
                        axislabel_opts=opts.LabelOpts(is_show=True, formatter="{value}"),
                        splitline_opts=opts.SplitLineOpts(is_show=False),
                    ),
                    yaxis_opts=opts.AxisOpts(
                        grid_index=x_index,
                        is_scale=True,
                        min_="dataMin",
                        max_="dataMax",
                        splitline_opts=opts.SplitLineOpts(is_show=False),
                        axislabel_opts=opts.LabelOpts(is_show=True),
                    ),
                    legend_opts=opts.LegendOpts(pos_top=f"{top + 1}%"),
                )

                # 添加副图到网格
                grid.add(
                    sub,
                    grid_opts=opts.GridOpts(
                        pos_top=f"{top}%",
                        height=f"{sub_height}%",
                        pos_left="3%",
                        pos_right="1%",
                    ),
                    is_control_axis_index=False  # 不由 grid 自动分配 xaxis_index
                )

        return grid


if __name__ == "__main__":
    import pandas as pd
    import numpy as np
    from pyecharts.globals import ThemeType


    # ===============================
    # 数据准备
    def cal_sma(n, data):
        return np.convolve(data, np.ones(n) / n, mode='valid').tolist() + [0] * (n - 1)


    def cal_bbands(data):
        mid = np.array(cal_sma(20, data))
        std = np.std(data)
        up = mid + 2 * std
        down = mid - 2 * std
        return {
            "bbands_up": up.tolist(),
            "bbands_mid": mid.tolist(),
            "bbands_down": down.tolist(),
        }


    # 创建假数据
    def generate_fake_kline_data(n=100):
        np.random.seed(42)
        base_price = 100
        dates = pd.date_range(end=pd.Timestamp.today(), periods=n).strftime('%Y-%m-%d').tolist()
        open_prices = np.random.normal(loc=base_price, scale=2, size=n)
        close_prices = open_prices + np.random.normal(loc=0, scale=2, size=n)
        high_prices = np.maximum(open_prices, close_prices) + np.random.rand(n)
        low_prices = np.minimum(open_prices, close_prices) - np.random.rand(n)
        volume = np.random.randint(1000, 5000, size=n)

        df = pd.DataFrame({
            "date": dates,
            "open": open_prices,
            "close": close_prices,
            "high": high_prices,
            "low": low_prices,
            "vol": volume
        })
        return df


    # 创建测试数据
    df = generate_fake_kline_data()
    df['date'] = pd.to_datetime(df['date'])
    sma10 = cal_sma(10, df["close"])
    sma15 = cal_sma(15, df["close"])
    date = df["date"]
    macd = np.random.randn(len(df['close'])).tolist()
    bar_data = df[['date']].copy()
    bar_data['macd'] = macd
    bar_data.iloc[0:5] = np.nan

    # ===============================
    # 主图
    kline = create_kline(df)
    # 叠加主图的线
    sma10_p = create_line(df['date'], sma10, y_label='sma10')
    sma15_p = create_line(df['date'], sma15, y_label='sma15')
    # 副图（MACD柱状图）
    macd_bar = create_bar(bar_data['date'], bar_data['macd'], y_label='macd')
    # 使用 ChartManager 构建图表
    manager = ChartManager(theme=ThemeType.DARK)
    manager.set_main(kline)
    manager.add_to_main(sma10_p)
    manager.add_to_main(sma15_p)
    manager.add_sub_chart(macd_bar)
    manager.set_datazoom_range(70, 100)
    manager.render("example_chart.html")

    print("示例图已生成：example_chart.html")

    k_line = ChartManager(theme=ThemeType.DARK)
    k_line.set_main(kline)
    k_line.set_datazoom_range(70, 100)
    k_line.add_sub_chart(macd_bar)

    sma_p = sma10_p.overlap(sma15_p)
    sma = ChartManager(theme=ThemeType.DARK)
    sma.set_main(sma_p)
    sma.set_datazoom_range(70, 100)

    # tab用于分页
    basic_plot = Tab()
    basic_plot.add(k_line.output(), "equity_plot")
    basic_plot.add(sma.output(), "profit_plot")
    basic_plot.render("basic_plot.html")


