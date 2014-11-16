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
using System.Windows.Navigation;
using System.Windows.Shapes;
using Fluent;
using TmFramework.TmStorage;
using Microsoft.Win32;
using System.Runtime.Serialization.Formatters.Binary;
using System.IO;
using System.Windows.Threading;

namespace SimpleImageStorage
{
    /// <summary>
    /// Interaction logic for MainWindow.xaml
    /// </summary>
    public partial class MainWindow : Fluent.RibbonWindow
    {
        #region Fields
        private Storage storage = null;
        private List<ImageInfo> items = null;
        private DispatcherTimer timer;
        private Guid fileListId = new Guid(1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
        #endregion

        #region Construction
        public MainWindow()
        {
            InitializeComponent();

            imageList.DisplayMemberPath = "DisplayName";
            imageList.SelectedValuePath = "StreamId";
            UpdateTransactionButtonStates();

            timer = new DispatcherTimer(
                new TimeSpan(0, 0, 1),
                DispatcherPriority.Normal,
                (o, e) =>
                {
                    if (storage != null)
                    {
                        amountReadTextBlock.Text = storage.Statistics.BytesRead.ToString("N0");
                        amountWrittenTextBlock.Text = storage.Statistics.BytesWritten.ToString("N0");
                    }
                    else
                    {
                        amountReadTextBlock.Text = "";
                        amountWrittenTextBlock.Text = "";
                    }
                },
                this.Dispatcher);
        }
        #endregion

        #region Private methods
        private void UpdateUI()
        {
            if (storage != null && !storage.IsClosed)
            {
                if (storage.ContainsStream(fileListId))
                {
                    imageList.ItemsSource = null;
                    imageList.ItemsSource = items;
                }
            }
            else
            {
                imageList.ItemsSource = null;
                imageContainer.Source = null;
            }
        }
        private void SaveImageList()
        {
            if (items != null)
            {
                storage.StartTransaction();

                BinaryFormatter f = new BinaryFormatter();
                if (!storage.ContainsStream(fileListId))
                    storage.CreateStream(fileListId);

                using (MemoryStream ms = new MemoryStream())
                {
                    using (Stream s = storage.OpenStream(fileListId))
                    {
                        s.Position = 0;
                        f.Serialize(ms, items);

                        byte[] buf = ms.GetBuffer();
                        s.Write(buf, 0, (int)ms.Length);
                    }
                }

                storage.CommitTransaction();
            }
        }
        private void LoadImageList()
        {
            if (storage.ContainsStream(fileListId))
            {
                // Load stream table
                BinaryFormatter f = new BinaryFormatter();
                using (Stream s = storage.OpenStream(fileListId))
                {
                    if (s.Length > 0)
                    {
                        s.Position = 0;
                        items = (List<ImageInfo>)f.Deserialize(s);
                    }
                    else
                        items = new List<ImageInfo>();
                }
            }
        }
        private void AddImages(IEnumerable<string> files)
        {
            if (files.Count() > 0)
            {
                if (items == null)
                    items = new List<ImageInfo>();

                //IsEnabled = false;
                //ProgressDialog dialog = new ProgressDialog("Progress", "Adding files...");
                //dialog.Show();
                try
                {
                    int count = files.Count();
                    int index = 1;

                    storage.StartTransaction();
                    foreach (string file in files)
                    {
                        using (StorageStream s = storage.CreateStream(Guid.NewGuid()))
                        {
                            using (FileStream fs = File.OpenRead(file))
                            {
                                byte[] buf = new byte[65536];

                                int l = fs.Read(buf, 0, buf.Length);
                                while (l > 0)
                                {
                                    s.Write(buf, 0, l);
                                    l = fs.Read(buf, 0, buf.Length);
                                }
                            }

                            items.Add(new ImageInfo()
                            {
                                Name = System.IO.Path.GetFileName(file),
                                Size = (int)s.Length,
                                StreamId = s.StreamId
                            });
                        }
                        //dialog.Progress = (double)index * 100d / (double)count;
                        index++;
                    }

                    SaveImageList();
                    UpdateUI();
                    RefreshMap(imageList.SelectedValue as Guid?);

                    storage.CommitTransaction();
                }
                finally
                {
                    //dialog.Close();
                    //IsEnabled = true;
                }
            }
        }
        private void ReplaceImage(string file, Guid streamId)
        {
            if (file.Count() > 0)
            {
                if (items == null)
                    items = new List<ImageInfo>();

                using (StorageStream s = storage.OpenStream(streamId))
                {
                    using (FileStream fs = File.OpenRead(file))
                    {
                        byte[] buf = new byte[65536];

                        int l = fs.Read(buf, 0, buf.Length);
                        while (l > 0)
                        {
                            s.Write(buf, 0, l);
                            l = fs.Read(buf, 0, buf.Length);
                        }
                    }

                    ImageInfo ii = items
                        .Where(i => i.StreamId == streamId)
                        .First();

                    ii.Name = System.IO.Path.GetFileName(file);
                    ii.Size = (int)s.Length;

                    s.SetLength(s.Position);
                }

                SaveImageList();
                UpdateUI();
                RefreshMap(imageList.SelectedValue as Guid?);
            }
        }
        private void RefreshMap(Guid? streamId)
        {
            mapCanvas.Children.Clear();

            if (storage != null)
            {
                mapCanvas.Background = Brushes.Gray;
                
                List<SegmentExtent> extents = storage.GetFreeSpaceExtents();
                DrawExtents(extents, Brushes.Green);

                if (streamId.HasValue && storage.ContainsStream(streamId.Value))
                {
                    extents = storage.GetStreamExtents(streamId.Value);

                    DrawExtents(extents, Brushes.Navy);
                }
            }
            else
            {
                mapCanvas.Background = Brushes.White;
            }

        }
        private void DrawExtents(List<SegmentExtent> extents, Brush brush)
        {
            if (storage.Statistics.StorageSize == 0)
                return;

            double k = (double)mapCanvas.ActualWidth / (double)storage.Statistics.StorageSize;

            foreach (var e in extents)
            {
                double x = (double)e.Location * k;
                double w = Math.Max((double)e.Size * k, 1d);

                Rectangle rect = new Rectangle();
                rect.Width = w;
                rect.Height = mapCanvas.ActualHeight;
                rect.SetValue(Canvas.LeftProperty, x);
                rect.SetValue(Canvas.TopProperty, 0d);
                rect.RadiusX = 6;
                rect.RadiusY = 3;
                rect.Fill = brush;

                mapCanvas.Children.Add(rect);
            }
        }
        private void DeleteSelectedImage()
        {
            Guid? streamId = imageList.SelectedValue as Guid?;

            if (streamId.HasValue)
            {
                int index = imageList.SelectedIndex;
                storage.DeleteStream(streamId.Value);
                ImageInfo ii = items
                    .Where(i => i.StreamId == streamId.Value)
                    .First();
                items.Remove(ii);
                SaveImageList();
                UpdateUI();
                imageList.SelectedIndex = index;
                RefreshMap(imageList.SelectedValue as Guid?);
            }
        }
        #endregion

        #region Event handlers
        private void CreateStorageButton_Click(object sender, RoutedEventArgs e)
        {
            SaveFileDialog dialog = new SaveFileDialog();
            dialog.Filter = "Storage files|*.storage";
            dialog.CheckFileExists = false;
            dialog.Title = "Create image storage";
            bool? result = dialog.ShowDialog();

            if (result.HasValue && result.Value)
            {
                if (storage != null)
                    storage.Close();

                string logFilename = dialog.FileName + "log";

                if (File.Exists(dialog.FileName))
                    File.Delete(dialog.FileName);
                if (File.Exists(logFilename))
                    File.Delete(logFilename);

                storage = new Storage(dialog.FileName, logFilename);
                storage.TransactionStateChanged += Storage_TransactionStateShanged;
                storage.CreateStream(fileListId);

                RefreshMap(null);
                UpdateTransactionButtonStates();
            }
        }

        private void Storage_TransactionStateShanged(object sender, TransactionStateChangedEventArgs e)
        {
            if (e.TransactionStateChangeType == TransactionStateChangeType.Rollback)
            {
                LoadImageList();
                UpdateUI();
                RefreshMap(imageList.SelectedValue as Guid?);
            }

            UpdateTransactionButtonStates();
        }

        private void UpdateTransactionButtonStates()
        {
            StartTransactionButton.IsEnabled = storage != null && !storage.InTransaction;
            CommitTransactionButton.IsEnabled = storage != null && storage.InTransaction;
            RollbackTransactionButton.IsEnabled = storage != null && storage.InTransaction;
        }
        private void OpenStorageButton_Click(object sender, RoutedEventArgs e)
        {
            OpenFileDialog dialog = new OpenFileDialog();
            dialog.Filter = "Storage files|*.storage";
            dialog.CheckFileExists = true;
            dialog.Title = "Open image storage";
            bool? result = dialog.ShowDialog();

            if (result.HasValue && result.Value)
            {
                if (storage != null)
                    storage.Close();

                storage = new Storage(dialog.FileName, dialog.FileName + "log");
                storage.TransactionStateChanged += Storage_TransactionStateShanged;

                LoadImageList();
                UpdateUI();
                RefreshMap(null);
                UpdateTransactionButtonStates();
            }
        }
        private void imageList_SelectionChanged(object sender, SelectionChangedEventArgs e)
        {
            if (storage != null)
            {
                Guid? streamid = imageList.SelectedValue as Guid?;
                if (streamid.HasValue && storage.ContainsStream(streamid.Value))
                {
                    Stream storageStream = storage.OpenStream(streamid.Value);
                    try
                    {
                        // Read all bytes first to memory, then load image from it
                        // If data is not read into memory first, then image won't load correctly but only partially. I don't know why
                        // this happens but there seems that this is not a bug in TmStorage. In both cases, when loading directly
                        // from stream or from memory, first 48 bytes read are exactly the same. The next reads are different.
                        // When reading from memory only one read is executed to read all the image data. If loading directly from
                        // stream, many reads are made in small quentities and it never reaches the end of the stream.
                        byte[] buf = new byte[(int)storageStream.Length];
                        storageStream.Read(buf, 0, buf.Length);

                        MemoryStream tmpStream = new MemoryStream(buf);

                        BitmapImage img = new BitmapImage();
                        storageStream.Position = 0;
                        img.BeginInit();
                        img.StreamSource = tmpStream;// storageStream;
                        img.EndInit();
                        imageContainer.Source = img;
                        imageContainer.Stretch = Stretch.Uniform;
                    }
                    catch
                    {
                        imageContainer.Source = null;
                    }

                    RefreshMap(streamid);
                }
                else
                    imageContainer.Source = null;
            }
        }
        private void AddImagesButton_Click(object sender, RoutedEventArgs e)
        {
            if (storage != null)
            {
                OpenFileDialog d = new OpenFileDialog();
                d.Filter = "Images(bmp,jpg,png,tif)|*.jpg;*.bmp;*.png;*.tif";
                d.Multiselect = true;
                bool? r = d.ShowDialog();
                if (r.HasValue && r.Value)
                {
                    AddImages(d.FileNames);
                }
            }
        }
        private void RibbonWindow_SizeChanged(object sender, SizeChangedEventArgs e)
        {
            RefreshMap(imageList.SelectedValue as Guid?);
        }
        private void DeleteImageButton_Click(object sender, RoutedEventArgs e)
        {
            DeleteSelectedImage();
        }
        private void imageList_KeyDown(object sender, KeyEventArgs e)
        {
            if (e.Key == Key.Delete)
                DeleteSelectedImage();
        }
        private void ReplaceImageButton_Click(object sender, RoutedEventArgs e)
        {
            Guid? streamId = imageList.SelectedValue as Guid?;

            if (streamId.HasValue)
            {
                OpenFileDialog d = new OpenFileDialog();
                d.Filter = "Images(bmp,jpg,png,tif)|*.jpg;*.bmp;*.png;*.tif";
                bool? r = d.ShowDialog();
                if (r.HasValue && r.Value)
                {
                    ReplaceImage(d.FileName, streamId.Value);
                }                

                UpdateUI();
                RefreshMap(imageList.SelectedValue as Guid?);
            }
        }
        private void StartTransactionButton_Click(object sender, RoutedEventArgs e)
        {
            if (storage != null)
                storage.StartTransaction();
        }
        private void CommitTransactionButton_Click(object sender, RoutedEventArgs e)
        {
            if (storage != null)
                storage.CommitTransaction();
        }
        private void RollbackTransactionButton_Click(object sender, RoutedEventArgs e)
        {
            if (storage != null)
                storage.RollbackTransaction();
        }
        private void TruncateStorageButton_Click(object sender, RoutedEventArgs e)
        {
            storage.TruncateStorage();
            RefreshMap(imageList.SelectedValue as Guid?);
        }
        private void RibbonWindow_Closing(object sender, System.ComponentModel.CancelEventArgs e)
        {
            if (storage != null)
                storage.Close();
        }
        #endregion

        private void imageList_DragEnter(object sender, DragEventArgs e)
        {
            e.Effects = DragDropEffects.Copy;
        }

        private void imageList_Drop(object sender, DragEventArgs e)
        {
            if (e.Data.GetDataPresent(DataFormats.FileDrop))
            {
                string[] files = (string[])e.Data.GetData(DataFormats.FileDrop);

                AddImages(files);
            }
        }
    }

    [Serializable]
    class ImageInfo
    {
        public Guid StreamId { get; set; }
        public string Name { get; set; }
        public int Size { get; set; }
        public string DisplayName
        {
            get
            {
                return string.Format("{0} ({1:N0} kB)", Name, Size / 1024);
            }
        }
    }
}
