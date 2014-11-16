using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using System.Windows.Documents;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Imaging;
using System.Windows.Shapes;

namespace SimpleImageStorage
{
    /// <summary>
    /// Interaction logic for ProgressDialog.xaml
    /// </summary>
    public partial class ProgressDialog : Window
    {
        public ProgressDialog(string title, string text)
        {
            InitializeComponent();

            Title = title;
            progressTextBlock.Text = text;
        }

        public double Progress
        {
            get { return progressBar.Value; }
            set
            {
                progressBar.SetValue(ProgressBar.ValueProperty, value);
                //progressBar.Value = value;
            }
        }
    }
}
