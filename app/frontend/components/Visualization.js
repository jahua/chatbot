import React from 'react';
import Plot from 'react-plotly.js';

const Visualization = ({ data }) => {
    if (!data || !data.type) {
        return null;
    }

    const renderVisualization = () => {
        switch (data.type) {
            case 'visitor_comparison':
                return renderVisitorComparison(data.data);
            case 'peak_period':
                return renderPeakPeriod(data.data);
            case 'spending_analysis':
                return renderSpendingAnalysis(data.data);
            case 'trend_analysis':
                return renderTrendAnalysis(data.data);
            default:
                return renderDefault(data.data);
        }
    };

    const renderVisitorComparison = (vizData) => {
        const { chart, summary } = vizData;
        return (
            <div className="visualization-container">
                <Plot
                    data={[
                        {
                            x: chart.dates,
                            y: chart.swiss_tourists,
                            type: 'scatter',
                            mode: 'lines',
                            name: 'Swiss Tourists'
                        },
                        {
                            x: chart.dates,
                            y: chart.foreign_tourists,
                            type: 'scatter',
                            mode: 'lines',
                            name: 'Foreign Tourists'
                        }
                    ]}
                    layout={{
                        title: 'Swiss vs Foreign Tourists Comparison',
                        xaxis: { title: 'Date' },
                        yaxis: { title: 'Number of Visitors' }
                    }}
                />
                <div className="summary-stats">
                    <h3>Summary Statistics</h3>
                    <div className="stats-grid">
                        <div className="stat-item">
                            <label>Total Swiss Tourists:</label>
                            <span>{summary.total_swiss.toLocaleString()}</span>
                        </div>
                        <div className="stat-item">
                            <label>Total Foreign Tourists:</label>
                            <span>{summary.total_foreign.toLocaleString()}</span>
                        </div>
                        <div className="stat-item">
                            <label>Average Swiss Tourists/Day:</label>
                            <span>{summary.avg_swiss.toLocaleString(undefined, { maximumFractionDigits: 0 })}</span>
                        </div>
                        <div className="stat-item">
                            <label>Average Foreign Tourists/Day:</label>
                            <span>{summary.avg_foreign.toLocaleString(undefined, { maximumFractionDigits: 0 })}</span>
                        </div>
                    </div>
                </div>
            </div>
        );
    };

    const renderPeakPeriod = (vizData) => {
        const { chart_data, peak_info, monthly_stats } = vizData;
        return (
            <div className="visualization-container">
                <Plot
                    data={[
                        {
                            x: chart_data.x,
                            y: chart_data.y,
                            type: 'scatter',
                            mode: 'lines',
                            name: 'Total Visitors'
                        }
                    ]}
                    layout={{
                        title: chart_data.title,
                        xaxis: { title: 'Date' },
                        yaxis: { title: 'Number of Visitors' }
                    }}
                />
                <div className="peak-info">
                    <h3>Peak Information</h3>
                    {peak_info.top_peaks.map((peak, index) => (
                        <div key={index} className="peak-item">
                            <strong>Peak {index + 1}:</strong>
                            <span>{peak.date}: {peak.visitors.toLocaleString()} visitors</span>
                        </div>
                    ))}
                    <div className="busiest-month">
                        <strong>Busiest Month:</strong>
                        <span>{peak_info.busiest_month} (Avg: {peak_info.busiest_month_avg.toLocaleString()} visitors/day)</span>
                    </div>
                </div>
            </div>
        );
    };

    const renderSpendingAnalysis = (vizData) => {
        const { chart, summary } = vizData;
        return (
            <div className="visualization-container">
                <Plot
                    data={[
                        {
                            x: chart.industries,
                            y: chart.values,
                            type: 'bar',
                            name: 'Spending by Industry'
                        }
                    ]}
                    layout={{
                        title: 'Industry Spending Analysis',
                        xaxis: { title: 'Industry' },
                        yaxis: { title: chart.column_name }
                    }}
                />
                <div className="summary-stats">
                    <h3>Summary</h3>
                    <div className="stats-grid">
                        <div className="stat-item">
                            <label>Total Amount:</label>
                            <span>${summary.total.toLocaleString()}</span>
                        </div>
                        <div className="stat-item">
                            <label>Average Amount:</label>
                            <span>${summary.average.toLocaleString()}</span>
                        </div>
                        <div className="stat-item">
                            <label>Top Industry:</label>
                            <span>{summary.top_industry}</span>
                        </div>
                        <div className="stat-item">
                            <label>Top Amount:</label>
                            <span>${summary.top_amount.toLocaleString()}</span>
                        </div>
                    </div>
                </div>
            </div>
        );
    };

    const renderTrendAnalysis = (vizData) => {
        const { chart, statistics } = vizData;
        const series = Object.entries(chart.series)
            .filter(([_, values]) => values !== null)
            .map(([name, values]) => ({
                x: chart.dates,
                y: values,
                type: 'scatter',
                mode: 'lines',
                name: name.split('_').map(word => word.charAt(0).toUpperCase() + word.slice(1)).join(' ')
            }));

        return (
            <div className="visualization-container">
                <Plot
                    data={series}
                    layout={{
                        title: 'Tourism Trends',
                        xaxis: { title: 'Date' },
                        yaxis: { title: 'Number of Visitors' }
                    }}
                />
                {statistics && (
                    <div className="trend-stats">
                        <h3>Statistics</h3>
                        <div className="stats-grid">
                            {Object.entries(statistics).map(([key, value]) => (
                                <div key={key} className="stat-item">
                                    <label>{key.split('_').map(word => word.charAt(0).toUpperCase() + word.slice(1)).join(' ')}:</label>
                                    <span>{value.toLocaleString()}</span>
                                </div>
                            ))}
                        </div>
                    </div>
                )}
            </div>
        );
    };

    const renderDefault = (vizData) => {
        const { chart, summary } = vizData;
        if (!chart || !chart.dates) {
            return null;
        }

        const series = Object.entries(chart)
            .filter(([key, _]) => key !== 'dates')
            .map(([name, values]) => ({
                x: chart.dates,
                y: values,
                type: 'scatter',
                mode: 'lines',
                name: name.split('_').map(word => word.charAt(0).toUpperCase() + word.slice(1)).join(' ')
            }));

        return (
            <div className="visualization-container">
                <Plot
                    data={series}
                    layout={{
                        title: 'Data Visualization',
                        xaxis: { title: 'Date' },
                        yaxis: { title: 'Value' }
                    }}
                />
                {summary && (
                    <div className="summary-stats">
                        <h3>Summary Statistics</h3>
                        <pre>{JSON.stringify(summary, null, 2)}</pre>
                    </div>
                )}
            </div>
        );
    };

    return (
        <div className="visualization-wrapper">
            {renderVisualization()}
        </div>
    );
};

export default Visualization; 